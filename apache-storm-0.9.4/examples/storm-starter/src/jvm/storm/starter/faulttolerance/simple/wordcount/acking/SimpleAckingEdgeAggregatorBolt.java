package storm.starter.faulttolerance.simple.wordcount.acking;

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
import backtype.storm.utils.Utils;

public class SimpleAckingEdgeAggregatorBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private static long WINDOW_LENGTH = 2000L;	// in msec
	
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX = 1;
	
	// output stream on which tuples will be emitted from this bolt
	private String outputStream_;
	
	// lastEmitTime 
	private long lastPushTime_ = System.currentTimeMillis();

	// delay vs Counts of words and its Tuples which are used to later
	// fail if ack is not received by PerEdgeAcking Storm
	// delay vs <word, count>
	HashMap<String, Integer> counts_ = new HashMap<String, Integer>();

	// word vs List<anchors>
	private HashMap<String, List<Tuple>> wordVsAnchors_ = new HashMap<String, List<Tuple>>();

	public SimpleAckingEdgeAggregatorBolt(String stream) {
		outputStream_ = stream;
	}
	
	public void pushUpdates() {
		long now = System.currentTimeMillis();
			// push updates only if last push time is more than delay
			boolean updatePushTime = false;
			if (now - lastPushTime_ >= WINDOW_LENGTH) {
				for (String word : counts_.keySet()) {
					List<Tuple> anchors = new ArrayList<Tuple>();
					if(wordVsAnchors_.containsKey(word)) {
						anchors = wordVsAnchors_.get(word);
					}
					emitTupleOnStream(anchors, new Values(word, counts_.get(word)), outputStream_);
					wordVsAnchors_.remove(word);
					updatePushTime = true;
					
				}
				// reset the counts after pushing
				if(updatePushTime) {
					counts_.clear();
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
		
		// this is to kind of achieve randomness as emitted by a
		// realistic source like twitter or some data feed
		Utils.sleep(500);

		String word = tuple.getString(MESSAGE_INDEX);
		
		// we need to populate the anchors list belonging to this word
		// these anchors will later be used to fail the upstream if an
		// ack for this message is not received within timeout
		List<Tuple> l = new ArrayList<Tuple>(Arrays.asList(tuple)); 
		if(wordVsAnchors_.containsKey(word)) {
			l.addAll(wordVsAnchors_.get(word));
		}
		wordVsAnchors_.put(word, l);
		
		// updating the count of the word received in the
		// delayVsCounts_ structure. this is used to push out the
		// counts when the push was >= delay time, as done is
		// pushUpdates()
		Integer count = 1;
		if(counts_.containsKey(word)) {
			count += counts_.get(word);
		}
		counts_.put(word, count);
		
		pushUpdates();
	}

	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStream_, new Fields("word", "count"));
		declarer.declare(new Fields("word", "count"));
	}
	
	public int getThisTaskId() {
		return super.getThisTaskId();
	}
}