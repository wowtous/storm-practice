package org.darebeat.wc.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * Created by darebeat on 10/16/16.
 */
public class PrintBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String msg = tuple.getString(0);
        if (msg != null) {
            System.out.println(msg);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
