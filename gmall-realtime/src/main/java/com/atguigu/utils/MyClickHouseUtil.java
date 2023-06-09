package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {
    public static <T> SinkFunction<T> getSinkFunction(String sql) {

        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //使用反射
                        Field[] declaredFields = t.getClass().getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            declaredField.setAccessible(true);
                            try {
                                //尝试获取字段上的注解
                                TransientSink annotation = declaredField.getAnnotation(TransientSink.class);
                                if (annotation != null){
                                    offset++;
                                    continue;
                                }
                                Object attrValue = declaredField.get(t);
                                //jdbc中索引从1开始 给占位符赋值
                                preparedStatement.setObject(i + 1 - offset, attrValue);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
