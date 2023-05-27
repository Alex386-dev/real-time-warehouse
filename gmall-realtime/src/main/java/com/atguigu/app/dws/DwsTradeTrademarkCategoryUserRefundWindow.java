package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取dwd层退单主题数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3.数据转换为JAVABEAN
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTmCategoryUserDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));
            return TradeTrademarkCategoryUserRefundBean
                    .builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });
        //TODO 4.关联维度表SKU INFO补充tm_id,cate3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeWithSkuDS = AsyncDataStream.unorderedWait(tradeTmCategoryUserDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimInfo) {
                        bean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        bean.setTrademarkId(dimInfo.getString("TM_ID"));
                    }
                },
                100, TimeUnit.SECONDS);
        //TODO 5.分组开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = tradeWithSkuDS.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean bean, long l) {
                                        return bean.getTs();
                                    }
                                }))
                .keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean bean) throws Exception {
                        return new Tuple3<>(
                                bean.getSkuId(),
                                bean.getCategory3Id(),
                                bean.getTrademarkId()
                        );
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<String, String, String> key, TimeWindow timeWindow, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                        TradeTrademarkCategoryUserRefundBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setRefundCount((long) (next.getOrderIdSet().size()));
                        next.setTs(System.currentTimeMillis());
                        collector.collect(next);
                    }
                });
        //TODO 6.关联维度表 补充其他字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withTrademarkDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) {
                        javaBean.setTrademarkName(jsonObj.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                100, TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith3DS = AsyncDataStream.unorderedWait(withTrademarkDS, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimInfo) {
                bean.setCategory3Name("NAME");
                bean.setCategory2Id("CATEGORY2_ID");
            }
        }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith2DS = AsyncDataStream.unorderedWait(reduceWith3DS, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimInfo) {
                bean.setCategory2Name("NAME");
                bean.setCategory1Id("CATEGORY1_ID");
            }
        }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith1DS = AsyncDataStream.unorderedWait(reduceWith2DS, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimInfo) {
                bean.setCategory3Name("NAME");
            }
        }, 100, TimeUnit.SECONDS);
        //TODO 7.写入CK
        reduceWith1DS.addSink(MyClickHouseUtil.getSinkFunction(
                "insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ));
        //TODO 8.启动任务
        env.execute();
    }
}
