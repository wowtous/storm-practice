package org.darebeat;

import redis.clients.jedis.Jedis;

/**
 * Created by darebeat on 10/17/16.
 */
public class RedisTest {
    private final static String host = "127.0.0.1";
    private final static int port = 6379;
    private static Jedis jedis;

    public static void main(String[] args) {
        connectToRedis();
        jedis.set("age", "11");
        System.out.println(jedis.get("name")+" age: "+ jedis.get("age"));
    }

    public static void connectToRedis(){
        jedis = new Jedis(host, port);
        jedis.connect();
    }
}
