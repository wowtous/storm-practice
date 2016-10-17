package org.darebeat.wc.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by darebeat on 10/16/16.
 * send a random message to process
 * extends BaseRichSpout or IRichSpout
 */
public class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    Random random;

    /**
     * init work
     * @param conf
     * @param context
     * @param collector
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        spoutOutputCollector = collector;
        random = new Random();
    }

    public void nextTuple() {
        Utils.sleep(2000);
        String[] sentences = new String[]{
                "jikexueyuan is a good school",
                "And if the golden sun",
                "four score and seven years ago",
                "storm hadoop spark hbase",
                "blogchong is a good man",
                "Would make my whole world bright",
                "blogchong is a good website",
                "storm would have to be with you",
                "Pipe to subprocess seems to be broken No output read",
                " You make me feel so happy",
                "For the moon never beams without bringing me dreams Of the beautiful Annalbel Lee",
                "Who love jikexueyuan and blogchong",
                "blogchong.com is Magic sites",
                "Ko blogchong swayed my leaves and flowers in the sun",
                "You love blogchong.com", "Now I may wither into the truth",
                "That the wind came out of the cloud",
                "at backtype storm utils ShellProcess",
                "Of those who were older than we"
        };

        // select a sentence in random from sentences
        String sentence = sentences[random.nextInt(sentences.length)];

        // publish tuple using emit()
        spoutOutputCollector.emit(new Values(sentence.trim().toLowerCase()));
    }

    // ack process
    public void ack(Object id){}
    // fail process
    public void fail(Object id){}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
