package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_DRIVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(jsonObject.getString("table"));
        //过滤字段
        if (tableProcess == null) {
            System.out.println("找不到对应的table 可能是不需要这张维度表或这张是事实表");
        } else {
            filterColumns(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
            //补充sinkTable并输出到流中
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            collector.collect(jsonObject);
        }

    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columns);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columnsList.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }

    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //获取并解析数据 CDC中建表相关的信息在after对应的value中
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
        //校验并建表
        check(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
        //放入状态并广播
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);
    }

    /*
    {"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id",
    "sink_extend":"xxx"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source",
    "ts_ms":1652513039549,"snapshot":"false","db":"gmall-211126-config","sequence":null,"table":"table_process",
    "server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1652513039551,
    "transaction":null}
     */
    private void check(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        //处理特殊字段
        try {
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            //拼接建表语句SQL create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equals(sinkPk)) {
                    createTableSql.append(sinkPk).append(" varchar primary key");
                } else {
                    createTableSql.append(columns[i]).append(" varchar");
                }
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }
            createTableSql.append(")").append(sinkExtend);
            //编译SQL
            System.out.println(createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            //执行SQL
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("建表失败" + sinkTable);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }
}
