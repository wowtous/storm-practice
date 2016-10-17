package org.darebeat.click.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.click.common.FieldNames;
import org.darebeat.click.common.IPResolver;
import org.json.simple.JSONObject;

import java.util.Map;

public class GeographyBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5413696588647790366L;

	private IPResolver resolver;
	private OutputCollector collector;
	
	public GeographyBolt(IPResolver resolver) {
		this.resolver = resolver;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		String ip = tuple.getStringByField(FieldNames.IP);
		JSONObject json = resolver.resolveIP(ip);

		String city = (String)json.get(FieldNames.CITY);
		String country = (String)json.get(FieldNames.COUNTRY_NAME);
		collector.emit(new Values(country, city));
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.COUNTRY,
				FieldNames.CITY
		));
	}

}
