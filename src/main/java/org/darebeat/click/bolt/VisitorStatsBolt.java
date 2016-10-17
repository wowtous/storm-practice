package org.darebeat.click.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.click.common.FieldNames;

import java.util.Map;

public class VisitorStatsBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	
	private int total = 0;
	private int uniqueCount = 0;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		boolean unique = Boolean.parseBoolean(tuple.getStringByField(FieldNames.UNIQUE));
		
		if ( unique ) {
			uniqueCount ++;
		}
		collector.emit(new Values(total, uniqueCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				FieldNames.TOTAL_COUNT,
				FieldNames.TOTAL_UNIQUE
		));
	}

}
