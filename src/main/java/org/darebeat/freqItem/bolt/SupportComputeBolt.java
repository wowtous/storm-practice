package org.darebeat.freqItem.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.freqItem.common.FieldNames;
import org.darebeat.freqItem.common.ItemPair;

import java.util.HashMap;
import java.util.Map;

public class SupportComputeBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8776034202913455949L;

	private OutputCollector collector;
	
	private Map<ItemPair, Integer> pairCounts;
	int pairTotalCount;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext,
			OutputCollector outputCollector) {
		this.collector = outputCollector;
		pairCounts = new HashMap<>();
		pairTotalCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		if ( tuple.getFields().get(0).equals(FieldNames.TOTAL_COUNT) ) {
			pairTotalCount = tuple.getIntegerByField(FieldNames.TOTAL_COUNT);
		}
		else if ( tuple.getFields().size() == 3 ) {
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			int pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);
			pairCounts.put(new ItemPair(item1, item2), pairCount);
		}
		else if ( tuple.getFields().get(0).equals(FieldNames.COMMAND) ) {
			for ( ItemPair itemPair : pairCounts.keySet() ) {
				double itemSupport = (double)(pairCounts.get(itemPair).intValue()) / pairTotalCount;
				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemSupport));
			}
		}
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2,
				FieldNames.SUPPORT
		));
	}
}
