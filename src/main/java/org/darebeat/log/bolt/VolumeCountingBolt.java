package org.darebeat.log.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.darebeat.log.common.FieldNames;
import org.darebeat.log.model.LogEntry;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

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
