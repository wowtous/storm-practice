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
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class FilterBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8776034202913455949L;

	private static final double SUPPORT_THRESHOLD = 0.01;
	private static final double CONFIDENCE_THRESHOLD = 0.01;
	
	private OutputCollector collector;
	private Jedis jedis;
	private String host;
	private int port;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		host = conf.get(ConfKeys.REDIS_HOST).toString();
		port = Integer.valueOf(conf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		String item1 = tuple.getStringByField(FieldNames.ITEM1);
		String item2 = tuple.getStringByField(FieldNames.ITEM2);
		ItemPair pair = new ItemPair(item1, item2);
		String pairString = pair.toString();

		double support = 0;
		double confidence = 0;
		if ( tuple.getFields().get(2).equals(FieldNames.SUPPORT) ) {
			support = tuple.getDoubleByField(FieldNames.SUPPORT);
			jedis.hset("supports", pairString, String.valueOf(support));
		} else if ( tuple.getFields().get(2).equals(FieldNames.CONFIDENCE) ) {
			confidence = tuple.getDoubleByField(FieldNames.CONFIDENCE);
			jedis.hset("confidences", pairString, String.valueOf(confidence));
		}
		
		if ( !jedis.hexists("supports", pairString) || 
				!jedis.hexists("confidences", pairString) ) {
			return;
		}

		support = Double.parseDouble(jedis.hget("supports", pairString));
		confidence = Double.parseDouble(jedis.hget("confidences", pairString));
		
		if ( support >= SUPPORT_THRESHOLD && confidence >= CONFIDENCE_THRESHOLD) {
			JSONObject pairValue = new JSONObject();
			pairValue.put(FieldNames.SUPPORT, support);
			pairValue.put(FieldNames.CONFIDENCE, confidence);
			jedis.hset("recommendedPairs", pair.toString(), pairValue.toJSONString());
			
			collector.emit(new Values(item1, item2, support, confidence));
		} else {
			jedis.hdel("recommendedPairs", pair.toString());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2,
				FieldNames.SUPPORT,
				FieldNames.CONFIDENCE
		));
	}
}
