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
import org.kie.api.io.ResourceType;
import org.kie.internal.KnowledgeBase;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.runtime.StatelessKnowledgeSession;

import java.util.Map;

@SuppressWarnings("deprecation")
public class LogRulesBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6075680405178677002L;

	public static Logger LOG = Logger.getLogger(LogRulesBolt.class);
	private StatelessKnowledgeSession ksession;
	private OutputCollector collector;
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
		KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
		kbuilder.add(ResourceFactory.newClassPathResource("/Syslog.drl", getClass()), ResourceType.DRL);
		if ( kbuilder.hasErrors() ) {
			LOG.error(kbuilder.getErrors().toString());
		}
		
		KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
		kbase.addKnowledgePackages(kbuilder.getKnowledgePackages());
		ksession = kbase.newStatelessKnowledgeSession();
	}

	@Override
	public void execute(Tuple input) {
		LogEntry entry = (LogEntry)input.getValueByField(FieldNames.LOG_ENTRY);
		if ( entry == null ) {
			LOG.fatal("Received null or incorrect value from tuple");
		}
		
		ksession.execute(entry);
		if ( !entry.isFilter() ) {
			LOG.debug("Emitting from Rules Bolt");
			collector.emit(new Values(entry));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.LOG_ENTRY));
	}

}
