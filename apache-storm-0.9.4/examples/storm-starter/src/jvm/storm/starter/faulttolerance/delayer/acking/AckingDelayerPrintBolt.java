package storm.starter.faulttolerance.delayer.acking;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class AckingDelayerPrintBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX_1 = 1;
	
	HashMap<String, Integer> counts_ = new HashMap<String, Integer>();
	
	@Override
	public void customPrepare(Map conf, TopologyContext context,
			OutputCollector collector) {
	}

	@Override
	public void customExecute(Tuple tuple) {
		
		// just like that
		Utils.sleep(100);
		
		// As Spout is sending directly to this bolt and it provides no
		// other fancy stuff other than the message
		// which is a simple string in this case.
		String word = tuple.getString(MESSAGE_INDEX_1);
		
		Integer count = 1;
		if(counts_.containsKey(word)) {
			count += counts_.get(word);
		}
		counts_.put(word, count);
		
		System.out.println("Super Count for characer {" + word + "} -> count {" + count + "}");
	}

	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
	}
	
}
