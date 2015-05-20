package storm.starter.faulttolerance.wordcount.acking;

import java.util.Map;
import java.util.Random;

import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class AckingSplitterBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX = 1;
	private Random _rand;
	private String outStream_;
	
	AckingSplitterBolt(String stream) {
		outStream_ = stream;
	}
	
	@Override
	public void customPrepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_rand = new Random();
	}

	@Override
	public void customExecute(Tuple tuple) {

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
				emitTupleOnStream(tuple, new Values(words[i]), outStream_);
			}
		}
		
	}

	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
		declarer.declareStream(outStream_, new Fields("word"));
		declarer.declare(new Fields("word"));
	}
	
}
