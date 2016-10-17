package org.darebeat.freqItem.common;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Created by darebeat on 10/17/16.
 */
public class OrderGenerator {
    private static final String REDIS_HOST = "127.0.0.1";
    private static final int REDIS_PORT = 6379;
    private static final int ORDER_COUNT = 30;

    private static final String[] ITEMS_NAME = new String[]{
            "milk","coffee","egg","flower","icecream",
            "wine","water","fish","golf","peer","CD"
    };

    private static Jedis jedis;
    private static Random random;

    public static void main(String[] args) {
        prepareRandom();
        connectToRedis();
        pushTuples();
        disconnectFromRedis();
    }

    private static void disconnectFromRedis() {
        if (jedis != null) jedis.disconnect();
    }

    private static void pushTuples() {
        for (int i=0;i<ORDER_COUNT;i++){
            JSONObject orderTuple = new JSONObject();
            JSONArray items = new JSONArray();
            Set<String> selectItems = new HashSet<>();

            for (int j=0;j<4;j++){
                JSONObject item = new JSONObject();

                while (true){
                    int itemIndex = random.nextInt(ITEMS_NAME.length);
                    String itemName = ITEMS_NAME[itemIndex];

                    if (!selectItems.contains(itemName)){
                        item.put(FieldNames.NAME,itemName);
                        item.put(FieldNames.COUNT,random.nextInt(1000));
                        items.add(item);
                        selectItems.add(itemName);
                        break;
                    }
                }
            }

            orderTuple.put(FieldNames.ID, UUID.randomUUID().toString());
            orderTuple.put(FieldNames.ITEMS,items);
            String jsonText = orderTuple.toJSONString();

            jedis.rpush("orders",jsonText);
        }
    }

    private static void connectToRedis() {
        jedis = new Jedis(REDIS_HOST,REDIS_PORT);
        jedis.connect();
    }

    private static void prepareRandom() {
        random = new Random(1000);
    }
}
