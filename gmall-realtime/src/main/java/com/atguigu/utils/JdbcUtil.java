package com.atguigu.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        //创建集合用于存放结果数据
        ArrayList<T> result = new ArrayList<>();
        //编译SQl 执行
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        //获取查询元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        //遍历结果集 每行数据转为T 加入结果集合
        while (resultSet.next()){
            T t = clz.newInstance();
            //列遍历
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);
                //判断是否需要下划线与驼峰的转换
                if (underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                BeanUtils.setProperty(t, columnName, value);
            }
            //T对象放入集合
            result.add(t);
        }
        //返回集合
        resultSet.close();
        preparedStatement.close();

        return result;
    }
}
