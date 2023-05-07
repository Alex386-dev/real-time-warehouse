package com.atguigu.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    /*
    {"database":"gmall-211126-flink","table":"base_trademark","type":"insert","ts":1652499161,"xid":167,"commit":true,
    "data":{"id":13,"tm_name":"atguigu","logo_url":"/aaa/aaa"}}
     */
    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {
        //拼接SQL语句 upsert into db.tn(id,name) values('1001','alex')
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + StringUtils.join(columns, ",") + ") values('"
                + StringUtils.join(values, "','") + "')";
        //预编译
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //执行
        preparedStatement.execute();
        connection.commit();
        //释放
        preparedStatement.close();
    }
}
