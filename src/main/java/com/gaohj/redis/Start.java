package com.gaohj.redis;

import com.gaohj.redis.cli.RedisCmd;
import picocli.CommandLine;

public class Start {

    public static void main(String[] args) {
        System.out.println("start-cmd");
        int exitCode = new CommandLine(new RedisCmd()).execute(args);
        System.exit(exitCode);
    }
}
