package org.darebeat.log.spout;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.darebeat.log.common.Conf;
import org.darebeat.log.common.FieldNames;
import org.darebeat.log.model.LogEntry;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class LogSpout extends BaseRichSpout {
	private static final long serialVersionUID = -3530375171365601524L;

	public static Logger LOG = Logger.getLogger(LogSpout.class);
	
	public static final String LOG_CHANNEL = "log";
	
	private Jedis jedis;
	private String host;
	private int port;
	private SpoutOutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		host = conf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(conf.get(Conf.REDIS_PORT_KEY).toString());
		this.collector = collector;
		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
	}

	@Override
	public void nextTuple() {
		String content = jedis.rpop(LOG_CHANNEL);
		
		if ( content == null || "nil".equals(content) ) {
			try {
				Thread.sleep(300);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		else {
			JSONObject obj = (JSONObject)JSONValue.parse(content);
			LogEntry entry = new LogEntry(obj);
			collector.emit(new Values(entry));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.LOG_ENTRY));
	}

}
