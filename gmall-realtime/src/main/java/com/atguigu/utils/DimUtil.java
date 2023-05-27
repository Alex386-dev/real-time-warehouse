package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        //先查询redis
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "dim:" + tableName + ":" + key;
        String dimJsonStr = jedis.get(redisKey);
        if (dimJsonStr != null){
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回维度数据
            return JSONObject.parseObject(dimJsonStr);
        }
        //查询数据
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName
                + " where id = '" + key + "'";
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        //将从Phoenix查询到的数据写入redis
        JSONObject dimInfo = queryList.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();
        //返回结果
        return queryList.get(0);
    }

    public static void delDimInfo(String tableName, String key){
        Jedis jedis = JedisUtil.getJedis();
        String jedisKey = "dim:" + tableName + ":" + key;
        jedis.del(jedisKey);
        jedis.close();
    }
}
