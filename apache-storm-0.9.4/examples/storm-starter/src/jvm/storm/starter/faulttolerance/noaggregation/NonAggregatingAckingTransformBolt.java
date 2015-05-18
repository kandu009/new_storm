package storm.starter.faulttolerance.noaggregation;

import java.util.Map;

import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class NonAggregatingAckingTransformBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX = 1;
	private String outStream_;
	
	NonAggregatingAckingTransformBolt(String stream) {
		outStream_ = stream;
	}
	
	@Override
	public void customPrepare(Map conf, TopologyContext context,
			OutputCollector collector) {
	}

	@Override
	public void customExecute(Tuple tuple) {

		Utils.sleep(100);

		String sentence = tuple.getString(MESSAGE_INDEX);
		if(sentence != null && !sentence.isEmpty()) {
			emitTupleOnStream(tuple, new Values(sentence.concat("!")), outStream_);
		}
		
	}

	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
		declarer.declareStream(outStream_, new Fields("word"));
		declarer.declare(new Fields("word"));
	}
	
}
