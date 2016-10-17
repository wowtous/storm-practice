package org.darebeat;

import org.darebeat.click.common.HttpIpResolver;
import org.json.simple.JSONObject;

/**
 * Created by darebeat on 10/17/16.
 */
public class JsonObjectTest {
    public static void main(String[] args) {
        JSONObject json = new HttpIpResolver().resolveIP("208.80.152.201");

        System.out.println(json);
    }
}
