package org.darebeat.wc.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by darebeat on 10/16/16.
 */
public class WordNormalizerBolt implements IRichBolt {
    private OutputCollector outputCollector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");

        for (String word : words){
            outputCollector.emit(new Values(word));
        }
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
