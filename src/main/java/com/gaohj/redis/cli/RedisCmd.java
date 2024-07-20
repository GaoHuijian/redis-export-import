package com.gaohj.redis.cli;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.gaohj.redis.utils.RedisUtils;
import picocli.CommandLine;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "redis export and import", version = "1.0.0", mixinStandardHelpOptions = true)
public class RedisCmd implements Runnable {

    @CommandLine.Option(names = {"-V", "--version"}, versionHelp = true, description = "display version info")
    boolean versionInfoRequested;

    @CommandLine.Option(names = {"-H", "--help"}, usageHelp = true, description = "display this help message")
    boolean usageHelpRequested;
    @CommandLine.Option(names = {"-h", "--host"}, description = "redis host", defaultValue = "127.0.0.1")
    private String host;

    @CommandLine.Option(names = {"-a", "--auth"}, description = "redis password")
    private String passsword;

    @CommandLine.Option(names = {"-p", "--port"}, description = "redis port", defaultValue = "6379")
    private int port;

    @CommandLine.Option(names = {"-k", "--keys"}, description = "redis keys")
    private String keys;

    @CommandLine.Option(names = {"-d", "--database"}, description = "redis database", defaultValue = "0")
    private int database;

    @CommandLine.Option(names = {"-t", "--type"}, description = "in导入 out导出 del导出并删除",  defaultValue = "out")
    private String type;

    @CommandLine.Option(names = {"-o", "--output"}, description = "output file", defaultValue = "output.txt")
    private String output;

    @CommandLine.Option(names = {"-i", "--input"}, description = "intput file", defaultValue = "output.txt")
    private String input;

    @Override
    public void run() {
        if(StrUtil.equals("out", type) && StrUtil.isBlank(keys)){
            System.out.println("导出时 keys 不能为空");
            return;
        }
        if(StrUtil.equals("del", type) && StrUtil.isBlank(keys)){
            System.out.println("删除时 keys 不能为空");
            return;
        }

        //判断输入文件是否存在
        if("in".equals(type) && !new File(input).exists()){
            System.out.println(input + "文件不存在");
            return;
        }

        if(database>15){
            System.out.println("database参数错误");
        }

        Jedis jedis = RedisUtils.getJedit(host, passsword, port, database);
        if(jedis == null){
            System.out.println("redis 连接失败");
            return;
        }else{
            System.out.println("redis 连接成功");
        }
        if("out".equals(type)){
            System.out.println("redis 开始导出");
            RedisUtils.outData(jedis, keys, output);
            System.out.println("redis 导出完成");
            return;
        }
        if("in".equals(type)){
            System.out.println("redis 开始导入");
            RedisUtils.inData(jedis, keys, input);
            System.out.println("redis 导入完成");
            return;
        }
        if("del".equals(type)){
            System.out.println("redis 开始删除");
            output = DateTime.now().toString("yyyyMMddHHmmss") + "dellog" + output;
            RedisUtils.outAndDelData(jedis, keys, output);
            System.out.println("redis 删除完成，删除数据已导出至" + output);
            return;
        }
        System.out.println("参数错误");
    }
}
