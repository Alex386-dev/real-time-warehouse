package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MySqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/*
CREATE TABLE KafkaTable (
  `database` STRING,
  `table` STRING,
  `type` STRING,
  `data` MAP<STRING,STRING>,
  `old` MAP<STRING,STRING>,
  `pt` as PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'topic_db',
  'properties.bootstrap.servers' = 'hadoop102:9092',
  'properties.group.id' = 'DwdTradeCartAdd',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
)
https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/kafka/
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.读取topic_db主题数据创建表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add"));
        //TODO 3.过滤加购数据
        Table cartInfo = tableEnv.sqlQuery("" +
                "select  " +
                "    `data`['id'] id,  " +
                "    `data`['user_id'] user_id,  " +
                "    `data`['sku_id'] sku_id,  " +
                "    `data`['cart_price'] cart_price,  " +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,  " +
                "    `data`['sku_name'] sku_name,  " +
                "    `data`['is_checked'] is_checked,  " +
                "    `data`['create_time'] create_time,  " +
                "    `data`['operate_time'] operate_time,  " +
                "    `data`['is_ordered'] is_ordered,  " +
                "    `data`['order_time'] order_time,  " +
                "    `data`['source_type'] source_type,  " +
                "    `data`['source_id'] source_id,  " +
                "    pt  " +
                "from topic_db  " +
                "where `database` = 'gmall'  " +
                "and `table` = 'cart_info'  " +
                "and (`type` = 'insert'  " +
                "or (`type` = 'update'   " +
                "    and   " +
                "    `old`['sku_num'] is not null   " +
                "    and   " +
                "    cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        //TODO 4.读取mysql的base_dic表 维度退化
        tableEnv.executeSql(MySqlUtil.getBaseDicLookUpDDL());
        //TODO 5.关联两张表
        tableEnv.createTemporaryView("cart_info_table", cartInfo);
        Table cartAddWithDicTable = tableEnv.sqlQuery("select " +
                "    ci.id, " +
                "    ci.user_id, " +
                "    ci.sku_id, " +
                "    ci.cart_price, " +
                "    ci.sku_num, " +
                "    ci.sku_name, " +
                "    ci.is_checked, " +
                "    ci.create_time, " +
                "    ci.operate_time, " +
                "    ci.is_ordered, " +
                "    ci.order_time, " +
                "    ci.source_type source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    ci.source_id " +
                "from cart_info_table ci " +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt as dic " +
                "on ci.source_type = dic.dic_code");
        //TODO 6.使用DDL方式创建加购事实表
        tableEnv.executeSql("create table dwd_cart_add ( " +
                "    `id` STRING, " +
                "    `user_id` STRING, " +
                "    `sku_id` STRING, " +
                "    `cart_price` STRING, " +
                "    `sku_num` STRING, " +
                "    `sku_name` STRING, " +
                "    `is_checked` STRING, " +
                "    `create_time` STRING, " +
                "    `operate_time` STRING, " +
                "    `is_ordered` STRING, " +
                "    `order_time` STRING, " +
                "    `source_type_id` STRING, " +
                "    `source_type_name` STRING, " +
                "    `source_id` STRING )" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));
        //TODO 7.写出到KAFKA
        tableEnv.executeSql("insert into dwd_cart_add select * from" + cartAddWithDicTable).print();
        env.execute();
    }
}
