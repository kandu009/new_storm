package storm.starter.faulttolerance.noaggregation;

import java.util.Map;
import java.util.Random;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class NonAggregatingTransformBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX = 0;
	private String outStream_;
	Boolean enableStormsTimeoutMechanism_;
	OutputCollector collector_;
	
	NonAggregatingTransformBolt(String stream) {
		outStream_ = stream;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		new Random();
		enableStormsTimeoutMechanism_ = context.enableStormDefaultTimeoutMechanism();
		collector_ = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		
		if (enableStormsTimeoutMechanism_) {
			collector_.ack(tuple);
		}

		Utils.sleep(100);

		String sentence = tuple.getString(MESSAGE_INDEX);
		if(sentence != null && !sentence.isEmpty()) {
			if (enableStormsTimeoutMechanism_) {
				collector_.emit(outStream_, tuple, new Values(sentence.concat("!")));
			} else {
				collector_.emit(outStream_, new Values(sentence.concat("!")));
			}
			System.out.println("Emitting transformed message {" + sentence.concat("!") + "} from Transform Bolt !");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outStream_, new Fields("word"));
		declarer.declare(new Fields("word"));
	}
	
}
