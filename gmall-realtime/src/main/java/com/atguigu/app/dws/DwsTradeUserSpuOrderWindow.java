package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeUserSpuOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取Kafka DWD层 下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3.转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(line -> JSONObject.parseObject(line));
        //TODO 4.按照order_detail_id分组
        KeyedStream<JSONObject, String> keyByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));
        //TODO 5.去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyByDetailIdDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                            lastValueState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject lastValue = this.lastValueState.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );
        //TODO 6.转换为JavaBean对象
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuDS = filterDS.map(jsonObject -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));
            return TradeUserSpuOrderBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderAmount(jsonObject.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });
        //TODO 7.关联SkuInfo维度表 补充SPU_id, tm_id, category3_id
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserWithSkuDS = AsyncDataStream.unorderedWait(tradeUserSpuDS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setSpuId(dimInfo.getString("SPU_ID"));
                tradeUserSpuOrderBean.setTrademarkId(dimInfo.getString("TM_ID"));
                tradeUserSpuOrderBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
            }
        }, 100, TimeUnit.SECONDS);
        //TODO 8.提取事件事件 生成watermark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithWaterMarkDS = tradeUserWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
                    @Override
                    public long extractTimestamp(TradeUserSpuOrderBean tradeUserSpuOrderBean, long l) {
                        return tradeUserSpuOrderBean.getTs();
                    }
                }
        ));
        //TODO 9.分组 开窗 聚合
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceDS = tradeUserSpuWithWaterMarkDS.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean value) throws Exception {
                        return new Tuple4<>(value.getUserId(), value.getSpuId(), value.getTrademarkId(), value.getCategory3Id());
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean value1, TradeUserSpuOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<TradeUserSpuOrderBean> iterable, Collector<TradeUserSpuOrderBean> collector) throws Exception {
                        TradeUserSpuOrderBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);
                    }
                });
        //TODO 10.关联SPU, tradeMark, category维表 补充相应信息
        //关联SPU
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithSpuDS = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getSpuId();
            }

            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setSpuName(dimInfo.getString("SPU_NAME"));
            }
        }, 100, TimeUnit.SECONDS);
        //关联TradeMark表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithTmDS = AsyncDataStream.unorderedWait(reduceWithSpuDS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setTrademarkName(dimInfo.getString("TM_NAME"));
            }
        }, 100, TimeUnit.SECONDS);
        //关联Cate3表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCate3DS = AsyncDataStream.unorderedWait(reduceWithTmDS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setCategory3Name(dimInfo.getString("name"));
                tradeUserSpuOrderBean.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
            }
        }, 100, TimeUnit.SECONDS);
        //关联Cate2表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCate2DS = AsyncDataStream.unorderedWait(reduceWithCate3DS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setCategory2Name(dimInfo.getString("name"));
                tradeUserSpuOrderBean.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
            }
        }, 100, TimeUnit.SECONDS);
        //关联Cate1表

        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCate1DS = AsyncDataStream.unorderedWait(reduceWithCate2DS, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                tradeUserSpuOrderBean.setCategory1Name(dimInfo.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 11.将数据写入clickHouse
        SinkFunction<TradeUserSpuOrderBean> sinkFunction = MyClickHouseUtil.<TradeUserSpuOrderBean>getSinkFunction(
                "insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        reduceWithCate1DS.addSink(sinkFunction);

        //TODO 12.启动
        env.execute();
    }
}
