package org.darebeat.click.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.click.common.ConfKeys;
import org.darebeat.click.common.FieldNames;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RepeatVisitBolt extends BaseRichBolt {

	private static final long serialVersionUID = -283193209123063833L;
	
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

	@Override
	public void execute(Tuple tuple) {
		String clientKey = tuple.getStringByField(FieldNames.CLIENT_KEY);
		String url = tuple.getStringByField(FieldNames.URL);
		String key = url + ":" + clientKey;
		String value = jedis.get(key);
		
		if ( value == null ) {
			jedis.set(key, "visited");
			collector.emit(new Values(
					clientKey,
					url,
					Boolean.TRUE.toString()
			));
		} else {
			collector.emit(new Values(
					clientKey,
					url,
					Boolean.FALSE.toString()
			));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				FieldNames.CLIENT_KEY,
				FieldNames.URL,
				FieldNames.UNIQUE
		));
	}

}
