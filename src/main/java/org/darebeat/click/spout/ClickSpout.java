package org.darebeat.click.spout;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.darebeat.click.common.ConfKeys;
import org.darebeat.click.common.FieldNames;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class ClickSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3757047085011759927L;

	public static Logger LOG = Logger.getLogger(ClickSpout.class);
	
	private Jedis jedis;
	private String host;
	private int port;
	private SpoutOutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, 
			TopologyContext topologyContext,
			SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		
		host = conf.get(ConfKeys.REDIS_HOST).toString();
		port = Integer.valueOf(
				conf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void nextTuple() {
		String content = jedis.rpop("count");
		
		if ( content == null || "nil".equals(content) ) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		else {
			JSONObject obj = (JSONObject)JSONValue.parse(content);
			String ip = obj.get(FieldNames.IP).toString();
			String url = obj.get(FieldNames.URL).toString();
			String clientKey = obj.get(FieldNames.CLIENT_KEY).toString();
			
			collector.emit(new Values(ip, url, clientKey));
		}
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.IP,
				FieldNames.URL,
				FieldNames.CLIENT_KEY
		));
	}

}
