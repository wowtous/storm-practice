package org.darebeat.freqItem;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.darebeat.freqItem.bolt.*;
import org.darebeat.freqItem.common.ConfKeys;
import org.darebeat.freqItem.common.FieldNames;
import org.darebeat.freqItem.spout.CommandSpout;
import org.darebeat.freqItem.spout.OrderSpout;

public class OrderTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		OrderTopology topology = new OrderTopology();
		
		if ( args != null && args.length > 1 ) {
			topology.runCluster(args[0], args[1]);
		}
		else {
			System.out.println(
					"Running in local mode" +
					"\nRedis ip missing for cluster run"
			);
			topology.runLocal(10000);
		}
	}

	public OrderTopology() {
		builder.setSpout("orderSpout", new OrderSpout(), 5);
		builder.setSpout("commandSpout", new CommandSpout(), 1);
		
		builder.setBolt("splitBolt", new SplitBolt(), 5).fieldsGrouping("orderSpout", new Fields(FieldNames.ID));
		
		builder.setBolt("pairCountBolt", new PairCountBolt(), 5).fieldsGrouping("splitBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
		builder.setBolt("pairTotalCountBolt", new PairTotalCountBolt(), 1).globalGrouping("splitBolt");

		builder.setBolt("supportComputeBolt", new SupportComputeBolt(), 5).
			allGrouping("pairTotalCountBolt").
			fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2)).
			allGrouping("commandSpout");
		builder.setBolt("confidenceComputeBolt", new ConfidenceComputeBolt(), 5).
			fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2)).
			allGrouping("commandSpout");
		
		builder.setBolt("filterBolt", new FilterBolt()).
			fieldsGrouping("supportComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2)).
			fieldsGrouping("confidenceComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
//		builder.setBolt("geographyBolt", 
//				new GeographyBolt(new HttpIpResolver()), 10)
//				.shuffleGrouping("clickSpout");
//		builder.setBolt("totalStats", new SplitBolt(), 1)
//			.globalGrouping("repeatBolt");
	}
	
	public TopologyBuilder getBuilder() {
		return builder;
	}

	public LocalCluster getLocalCluster() {
		return cluster;
	}
	
	private void runLocal(int runTime) {
		conf.setDebug(true);
		conf.put(ConfKeys.REDIS_HOST, "redis");
		conf.put(ConfKeys.REDIS_PORT, "6379");
		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, 
				builder.createTopology());
		
		if ( runTime > 0 ) {
			Utils.sleep(runTime);
			shutdownLocal();
		}
	}

	private void shutdownLocal() {
		if ( cluster != null ) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	private void runCluster(String name, String redisHost) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		conf.setNumWorkers(20);
		conf.put(ConfKeys.REDIS_HOST, redisHost);
		conf.put(ConfKeys.REDIS_PORT, "6379");
		StormSubmitter.submitTopology(name, conf,
				builder.createTopology());
	}

}
