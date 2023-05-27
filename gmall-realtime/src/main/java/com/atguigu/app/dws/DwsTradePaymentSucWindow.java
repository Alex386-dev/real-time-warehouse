package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradePaymentWindowBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.获取DWD层支付成功主题创建流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3.将数据转换为JSON
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(line -> JSONObject.parseObject(line));
        //TODO 4.按照order_detail_id分组
        KeyedStream<JSONObject, String> keyByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));
        //TODO 5.状态编程保留最新数据输出
        SingleOutputStreamOperator<JSONObject> latestDS = keyByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                out.collect(valueState.value());
                valueState.clear();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject state = valueState.value();
                if (state == null) {
                    valueState.update(jsonObject);
                    context.timerService().registerEventTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {
                    String stateRt = state.getString("row_op_ts");
                    String curRt = jsonObject.getString("row_op_ts");
                    //保留更晚的数据
                    int result = TimestampLtz3CompareUtil.compare(stateRt, curRt);
                    if (result != 1) {
                        valueState.update(jsonObject);
                    }
                }
            }
        });
        //TODO 6.提取事件时间 生成waterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = latestDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return DateFormatUtil.toTs(jsonObject.getString("call_back_time"), true);
                    }
                }));
        //TODO 7.按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWithWatermarkDS.keyBy(json -> json.getString("user_id"));
        //TODO 8.提取独立支付成功用户数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUidDS.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastDt", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                String lastDt = valueState.value();
                String curDt = jsonObject.getString("call_back_time").split(" ")[0];
                //当日独立用户数
                long pay = 0L;
                //新增支付用户数
                long newPay = 0L;
                if (lastDt == null) {
                    pay = 1L;
                    newPay = 1L;
                    valueState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    pay = 1L;
                    valueState.update(curDt);
                }
                if (pay == 1L) {
                    collector.collect(new TradePaymentWindowBean(
                            "", "",
                            pay, newPay, null
                    ));
                }
            }
        });
        //TODO 9.开窗聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                            @Override
                            public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                                return value1;
                            }
                        },
                        new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {
                                TradePaymentWindowBean next = iterable.iterator().next();
                                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                                next.setTs(System.currentTimeMillis());
                                collector.collect(next);
                            }
                        });
        //TODO 10.写入CK
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));
        //TODO 11.启动任务
        env.execute();
    }
}
