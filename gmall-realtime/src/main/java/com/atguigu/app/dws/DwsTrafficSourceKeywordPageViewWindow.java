package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordBean;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.FlinkSQL读取KAFKA page_log 创建表并提取时间戳生成waterMark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_log(" +
                "   page map<string, string>," +
                "   ts bigint," +
                "   rt AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "   WATERMARK FOR rt AS rt - INTERVAL '2' SECOND" +
                ")" + MyKafkaUtil.getKafkaDDL(topic, groupId));
        //TODO 3.过滤出搜索数据  last_page_id是search && item_type是keyword && item不为null
        Table filterTable = tableEnv.sqlQuery("" +
                "select" +
                "page['item']," +
                "rt " +
                "from page_log" +
                "where" +
                "    page['last_page_id'] = 'search'" +
                "and page['item_type'] = 'keyword'" +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);
        //TODO 4.注册UDTF & 切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery(
                "SELECT word, rt " + "FROM filter_table, LATERAL TABLE(SplitFunction(item))");
        tableEnv.createTemporaryView("split_table", splitTable);
        //TODO 5.分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select" +
                "word keyword," +
                "'search' source," +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts," +
                "count(*) keyword_count" +
                "from" +
                "split_table" +
                "group by" +
                "word," +
                "TUMBLE(rt, INTERVAL '10' SECOND)");
        //TODO 6.将动态表转换为流
        DataStream<KeywordBean> appendStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        //TODO 7.将数据写出到CK

        //TODO 8.启动任务
        env.execute();
    }
}
