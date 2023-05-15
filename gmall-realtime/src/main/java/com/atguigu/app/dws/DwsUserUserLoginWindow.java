package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserLoginBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
度量值: 当日独立用户数 + 七日回流用户数
用户登录
1.uid不为空
并且
2.last_page_id为null 自动登陆 或 last_page_id为login 中途登陆
 */
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取Kafka页面日志主题 创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3.过滤转换数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (uid != null && (lastPageId == null || lastPageId.equals("login"))) {
                    collector.collect(jsonObject);
                }
            }
        });
        //TODO 4.提取事件时间生成waterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWaterMarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));
        //TODO 5.按照uid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWaterMarkDS.keyBy(json -> json.getJSONObject("common").getString("uid"));
        //TODO 6.使用状态编程 提取独立用户&七日回流用户
        SingleOutputStreamOperator<UserLoginBean> userLoginDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
            private ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_login_time", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<UserLoginBean> collector) throws Exception {
                //获取状态日期以及当前数据日期
                String lastLoginDt = lastLoginState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                //定义当日独立用户数 和 七日回流用户数
                long uv = 0;
                long backUv = 0;
                if (lastLoginDt == null) {
                    uv = 1L;
                    lastLoginState.update(curDt);
                } else {
                    if (!lastLoginDt.equals(curDt)) {
                        lastLoginState.update(curDt);
                        uv = 1;
                        if ((DateFormatUtil.toTs(curDt, false) - DateFormatUtil.toTs(lastLoginDt)) / (24 * 60 * 60 * 1000L) >= 8) {
                            backUv = 1L;
                        }
                    }
                }
                if (uv != 0L) {
                    collector.collect(new UserLoginBean("", "", backUv, uv, ts));
                }
            }
        });
        //TODO 7.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userLoginDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        return new UserLoginBean("", "",
                                value1.getBackCt() + value2.getBackCt(),
                                value1.getUuCt() + value2.getUuCt(),
                                value1.getTs());
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        UserLoginBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);
                    }
                });
        //TODO 8.写入CK
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_login_window values(?, ?, ?, ?, ?)"));
        //TODO 9.启动任务
        env.execute();
    }
}
