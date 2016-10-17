package org.darebeat.log.bolt;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.learningstorm.log.common.FieldNames;
import org.learningstorm.log.model.LogEntry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class VolumeCountingBolt extends BaseRichBolt {
	public static Logger LOG = Logger.getLogger(VolumeCountingBolt.class);
	private OutputCollector collector;
	
	public static final String FIELD_ROW_KEY = "RowKey";
	public static final String FIELD_COLUMN = "Column";
	public static final String FIELD_INCREMENT = "IncrementAmount";
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		LogEntry entry = (LogEntry)input.getValueByField(FieldNames.LOG_ENTRY);
		
		collector.emit(new Values(getMinuteForTime(entry.getTimestamp()), entry.getSource(), 1L));
	}

	private Object getMinuteForTime(Date time) {
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		
		return c.getTimeInMillis();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FIELD_ROW_KEY, FIELD_COLUMN, FIELD_INCREMENT));
	}

}
