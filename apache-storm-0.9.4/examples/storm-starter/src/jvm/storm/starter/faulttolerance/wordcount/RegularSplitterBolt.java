package storm.starter.faulttolerance.wordcount;

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

public class RegularSplitterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX = 0;
	private Random _rand;
	private String outStream_;
	Boolean enableStormsTimeoutMechanism_;
	OutputCollector collector_;
	
	RegularSplitterBolt(String stream) {
		outStream_ = stream;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_rand = new Random();
		enableStormsTimeoutMechanism_ = context.enableStormDefaultTimeoutMechanism();
		collector_ = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// this is to kind of achieve randomness as emitted by a
		// realistic
		// source like twitter or some data feed
		Utils.sleep(Math.abs(_rand.nextInt()) % 500);

		// As Spout is sending directly to this bolt and it provides no
		// other fancy stuff other than the message
		// which is a simple string in this case.
		String sentence = tuple.getString(MESSAGE_INDEX);
		
		if(sentence != null && !sentence.isEmpty()) {
			String[] words = sentence.split("[* *]+");
			for (int i = 0; i < words.length; ++i) {
				if (enableStormsTimeoutMechanism_) {
					collector_.emit(outStream_, tuple, new Values(words[i]));
				} else {
					collector_.emit(outStream_, new Values(words[i]));
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outStream_, new Fields("word"));
		declarer.declare(new Fields("word"));
	}
	
}
