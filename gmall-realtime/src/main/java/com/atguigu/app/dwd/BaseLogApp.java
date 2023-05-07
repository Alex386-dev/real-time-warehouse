package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启checkpoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/211126/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //TODO 2.消费Kafka topic_log主题数据
        String topic = "topic_log";
        String groupId = "BaseLogApp";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3.过滤掉非JSON格式的数据 将每行转成JSON对象
        OutputTag<String> dirtyTag = new OutputTag<>("dirty");
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    context.output(dirtyTag, s);
                }
            }
        });
        jsonObjDS.getSideOutput(dirtyTag).print("Dirty>>>>>");
        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 5.新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取新老标记
                String isNew = value.getJSONObject("common").getString("is_new");
                //获取时间戳 并转化为当前日期字符串
                Long timeStamp = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(timeStamp);
                //获取状态中的时间
                String lastDate = lastVisitState.value();
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else {
                        if (!lastDate.equals(curDate)) {
                            value.getJSONObject("common").put("isNew", "0");
                        }
                    }
                } else {
                    if (lastDate == null) {
                        lastVisitState.update(DateFormatUtil.toDate(timeStamp - 24 * 60 * 60 * 1000L));
                    }
                }
                return value;
            }
        });
        //TODO 6.侧输出流分流处理
        /*
        页面放到主流 启动 曝光 动作 错误放到侧输出流
         */
        OutputTag<String> startTag = new OutputTag<>("start");
        OutputTag<String> displayTag = new OutputTag<>("display");
        OutputTag<String> actionTag = new OutputTag<>("action");
        OutputTag<String> errorTag = new OutputTag<>("error");
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //错误
                String err = value.getString("err");
                if (err != null) {
                    context.output(errorTag, value.toJSONString());
                }
                value.remove("err");
                //启动
                String start = value.getString("start");
                if (start != null) {
                    context.output(startTag, value.toJSONString());
                } else {
                    //获取公共信息 页面id 时间戳
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    //曝光
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                    //动作
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            context.output(actionTag, action.toJSONString());
                        }
                    }
                    //页面写到主流
                    value.remove("actions");
                    value.remove("displays");
                    collector.collect(value.toJSONString());
                }
            }
        });
        //TODO 7.提取各分流数据

        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> errDS = pageDS.getSideOutput(errorTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);


        //TODO 8.各分流数据写入topic
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        errDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        //TODO 9.启动任务
        env.execute("BaseLogApp");
    }
}
