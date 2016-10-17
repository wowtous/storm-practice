package org.darebeat.wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.darebeat.wc.bolt.PrintBolt;
import org.darebeat.wc.bolt.WordCountBolt;
import org.darebeat.wc.bolt.WordNormalizerBolt;
import org.darebeat.wc.spout.RandomSentenceSpout;

/**
 * Created by darebeat on 10/16/16.
 */
public class WordCountTopology {
    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) {
        Config config = new Config();
        builder.setSpout("RandomSentence",new RandomSentenceSpout(),2);
        builder.setBolt("WordNormalizer",new WordNormalizerBolt(),2).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount",new WordCountBolt(),2).fieldsGrouping("WordNormalizer",new Fields("word"));
        builder.setBolt("Print",new PrintBolt(),1).shuffleGrouping("WordCount");

        config.setDebug(false);

        // run with local or cluster pattern
        if (args != null && args.length>0){
            config.setNumWorkers(1);
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordCount",config,builder.createTopology());
        }
    }

}
