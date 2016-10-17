package org.darebeat.click;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.darebeat.click.bolt.GeoStatsBolt;
import org.darebeat.click.bolt.GeographyBolt;
import org.darebeat.click.bolt.RepeatVisitBolt;
import org.darebeat.click.bolt.VisitorStatsBolt;
import org.darebeat.click.common.ConfKeys;
import org.darebeat.click.common.FieldNames;
import org.darebeat.click.common.HttpIpResolver;
import org.darebeat.click.spout.ClickSpout;

public class ClickTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		ClickTopology topology = new ClickTopology();
		
		if ( args != null && args.length > 1 ) {
			topology.runCluster(args[0], args[1]);
		} else {
			System.out.println("Running in local mode\nRedis ip missing for cluster run");
			topology.runLocal(10000);
		}
	}

	public ClickTopology() {
		builder.setSpout("clickSpout", new ClickSpout(), 10);
		builder.setBolt("repeatBolt", new RepeatVisitBolt(), 10).shuffleGrouping("clickSpout");
		builder.setBolt("geographyBolt", new GeographyBolt(new HttpIpResolver()), 10).shuffleGrouping("clickSpout");
		builder.setBolt("totalStats", new VisitorStatsBolt(), 1).globalGrouping("repeatBolt");
		builder.setBolt("geoStats", new GeoStatsBolt(), 10).fieldsGrouping("geographyBolt", new Fields(FieldNames.COUNTRY));
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
		cluster.submitTopology("test", conf, builder.createTopology());
		
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
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}

}
