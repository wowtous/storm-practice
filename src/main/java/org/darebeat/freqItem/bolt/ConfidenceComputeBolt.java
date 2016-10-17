package org.darebeat.freqItem.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.freqItem.common.ConfKeys;
import org.darebeat.freqItem.common.FieldNames;
import org.darebeat.freqItem.common.ItemPair;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class ConfidenceComputeBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	private Jedis jedis;
	private String host;
	private int port;
	
	private Map<ItemPair, Integer> pairCounts;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		host = conf.get(ConfKeys.REDIS_HOST).toString();
		port = Integer.valueOf(conf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
		
		pairCounts = new HashMap<>();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void execute(Tuple tuple) {
		if ( tuple.getFields().size() == 3 ) {
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			int pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);
			pairCounts.put(new ItemPair(item1, item2), pairCount);
		} else if ( tuple.getFields().get(0).equals(FieldNames.COMMAND) ) {
			for ( ItemPair itemPair : pairCounts.keySet() ) {
				int item1Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem1()));
				int item2Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem2()));
				double itemConfidence = pairCounts.get(itemPair).intValue();
				if ( item1Count < item2Count ) {
					itemConfidence /= item1Count;
				} else {
					itemConfidence /= item2Count;
				}
				
				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemConfidence));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2,
				FieldNames.CONFIDENCE
		));
	}
}
