package com.gaohj.redis.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.*;

public class RedisUtils {

    public static Jedis getJedit(String host, String pswd, int port, int database){
        try {
            Jedis jedis = new Jedis(host, port);
            if(StrUtil.isNotBlank(pswd)){
                jedis.auth(pswd);
            }
            if(database > 0){
                jedis.select(database);
            }
            return jedis;
        }catch (Exception e){
            return null;
        }

    }

    /**
     *
     * @param jedis
     * @param keys
     * @param output
     */
    public static void outData(Jedis jedis, String keys, String output) {
        Set<String> set = jedis.keys(keys);
        if(set==null || set.isEmpty()){
            System.out.println("无数据");
            return;
        }
        List<String> list = new ArrayList<String>();
        for (String key : set) {
            String type = jedis.type(key);
            String line = handleData(jedis, type, key);
            if(StrUtil.isNotBlank(line)){
                list.add(line);
            }
        }
        FileUtil.writeLines(list, new File(output), "UTF-8");
        System.out.println("导出成功");
        jedis.close();
    }

    private static String handleData(Jedis jedis, String type, String key) {
        String value = "";
        switch (type){
            case "none ":
                return "";

            case "string":
                value = jedis.get(key);
                break;

            case "list":
                Long len = jedis.llen(key);
                if(len>0){
                    List<String> values = jedis.lrange(key, 0, len);
                    value = JSONUtil.toJsonStr(values);
                }
                break;

            case "set":
                Set<String> values = jedis.smembers(key);
                value = JSONUtil.toJsonStr(values);
                break;

            case "zset":
                return "";

            case "hash":
                Map<String, String> map = jedis.hgetAll(key);
                value = JSONUtil.toJsonStr(map);
                break;

            default:
                break;
        }
        if(StrUtil.isNotBlank(value)){
            List<String> list = new ArrayList<>(3);
            list.add(key);
            list.add(type);
            list.add(value);
            return StrUtil.join(",", list);
        }
        return null;
    }

    /**
     *
     * @param jedis
     * @param keys
     * @param input
     */
    public static void inData(Jedis jedis, String keys, String input) {
        List<String> list = FileUtil.readLines(new File(input), "UTF-8");
        if(list!=null && !list.isEmpty()){
            for (String line : list) {
                int index1 = line.indexOf(",");
                String key = line.substring(0, index1);
                int index2 = line.indexOf(",", index1+1);
                String type = line.substring(index1+1, index2);
                String value = line.substring(index2+1);
                switch (type){
                    case "string":
                        jedis.set(key, value);
                        break;

                    case "list":
                        List<String> values1 = JSONUtil.toList(value, String.class);
                        jedis.lpush(key, ArrayUtil.toArray(values1, String.class));
                        break;

                    case "set":
                        List<String> values2 = JSONUtil.toList(value, String.class);
                        jedis.sadd(key, ArrayUtil.toArray(values2, String.class));
                        break;

                    case "hash":
                        JSONObject json = JSONUtil.parseObj(value);
                        Map<String,String> map = new HashMap<>();
                        for (String _key : json.keySet()) {
                            map.put(_key, json.getStr(_key));
                        }
                        jedis.hset(key, map);
                        break;

                    default:
                        break;

                }
            }
        }
        jedis.close();
    }


    /**
     * 导出并删除
     * @param jedis
     * @param keys
     * @param output
     */
    public static void outAndDelData(Jedis jedis, String keys, String output) {
        Set<String> set = jedis.keys(keys);
        if(set==null || set.isEmpty()){
            System.out.println("无数据");
            return;
        }
        List<String> list = new ArrayList<String>();
        for (String key : set) {
            String type = jedis.type(key);
            String line = handleData(jedis, type, key);
            if(StrUtil.isNotBlank(line)){
                list.add(line);
            }
            jedis.del(key);
        }
        FileUtil.writeLines(list, new File(output), "UTF-8");
        jedis.close();
    }
}
