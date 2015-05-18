package storm.starter.faulttolerance.noaggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NonAggregatingAckingDelayerBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX_1 = 1;
	
	// output stream on which tuples will be emitted from this bolt
	private String outputStream_;
	private static long WINDOW_LENGTH = 2000L;

	// lastEmitTime
	private Long lastPushTime_ = System.currentTimeMillis();

	// these are the words that are just delayed until the end of the current window
	// but there is no aggregation done, we will forward as many tuples as we receive

	// word vs anchor (i.e., the source tuple for this current tuple)
	private HashMap<String, List<Tuple>> wordVsAnchors_ = new HashMap<String, List<Tuple>>();

	public NonAggregatingAckingDelayerBolt(String stream) {
		outputStream_ = stream;
	}
	
	public void pushUpdates() {
		long now = System.currentTimeMillis();
		boolean updatePushTime = false;
		// push updates only if last push time is more than delay
		if (now - lastPushTime_ >= WINDOW_LENGTH) {
			for (String word : wordVsAnchors_.keySet()) {
				List<Tuple> anchors = wordVsAnchors_.get(word) == null ? new ArrayList<Tuple>() : wordVsAnchors_.get(word);
				for(Tuple anchor: anchors) {
					emitTupleOnStream(anchor, new Values(word), outputStream_);
				}
				System.out.println("Emitting delayed message {" + word + "} from Delayer Bolt !");
				updatePushTime = true;
			}
			// reset the counts after pushing
			if(updatePushTime) {
				wordVsAnchors_.clear();
				lastPushTime_ = now;
			}
		}
	}

	@Override
	public void customPrepare(Map conf, TopologyContext context,
			OutputCollector collector) {
	}

	@Override
	public void customExecute(Tuple tuple) {

		String word = tuple.getString(MESSAGE_INDEX_1); 
		
		// we need to populate the anchors list belonging to this character
		// these anchors will later be used to fail the upstream if an
		// ack for this message is not received within timeout
		List<Tuple> l = new ArrayList<Tuple>(Arrays.asList(tuple)); 
		if(wordVsAnchors_.containsKey(word)) {
			l.addAll(wordVsAnchors_.get(word));
		}
		wordVsAnchors_.put(word, l);

		pushUpdates();
	}
				
	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStream_, new Fields("word"));
		declarer.declare(new Fields("character"));
	}
	
}