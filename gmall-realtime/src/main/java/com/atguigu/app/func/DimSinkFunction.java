package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();
        //获取数据类型
        String type = value.getString("type");
        //如果为更新数据，需要删除redis中的数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        if (type.equals("update")){
            DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"));
        }
        //写出数据
        PhoenixUtil.upsertValues(connection, sinkTable, data);
        //归还连接
        connection.close();
    }

}
