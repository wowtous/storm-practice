package org.darebeat.wc.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.wc.util.MapSort;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by darebeat on 10/16/16.
 */
public class WordCountBolt implements IRichBolt {
    Map<String,Integer> map;
    private OutputCollector outputCollector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        map = new HashMap<String, Integer>();
        outputCollector = collector;
    }

    public void execute(Tuple tuple) {
        String str = tuple.getString(0);

        if (!map.containsKey(str)) {
            map.put(str,1);
        } else {
            map.put(str,map.get(str)+1);
        }

        map = MapSort.sortByValue(map);

        // Get the top N data
        int num = 8;
        int length = 0;

        if (num < map.keySet().size()){
            length = num;
        } else {
            length = map.keySet().size();
        }

        String word = null;
        int count = 0;
        for (String key : map.keySet()){
            if (count > length) {
                break;
            }

            if (count == 0) {
                word = "[" + key + ":" + map.get(key) + "]";
            } else {
                word += ",[" + key + ":" + map.get(key) + "]";
            }
            count++;
        }

        word = "The first " + num + ": "+ word;
        outputCollector.emit(new Values(word));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
