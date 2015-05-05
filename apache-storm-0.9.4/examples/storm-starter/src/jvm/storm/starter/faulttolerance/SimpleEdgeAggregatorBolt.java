package storm.starter.faulttolerance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SimpleEdgeAggregatorBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private static long WINDOW_LENGTH = 2000L;	// in msec
	
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX = 0;
	
	// output stream on which tuples will be emitted from this bolt
	private String outputStream_;
	
	// Counts of words and its Tuples which are used to later
	// <word, count>
	HashMap<String, Integer> counts_ = new HashMap<String, Integer>();
	// word vs List<anchors>
	private HashMap<String, List<Tuple>> wordVsAnchors_ = new HashMap<String, List<Tuple>>();
	
	private OutputCollector collector_;
	Boolean enableStormsTimeoutMechanism_;
	private long lastPushTime_ = System.currentTimeMillis();

	public SimpleEdgeAggregatorBolt(String stream) {
		outputStream_ = stream;
	}
	
	public void pushUpdates() {
		long now = System.currentTimeMillis();
		// push updates only if last push time is more than delay
		boolean updatePushTime = false;
		if (now - lastPushTime_ >= WINDOW_LENGTH) {
			for (String word : counts_.keySet()) {
				if (enableStormsTimeoutMechanism_) {
					List<Tuple> anchors = new ArrayList<Tuple>();
					if(wordVsAnchors_.containsKey(word)) {
						anchors = wordVsAnchors_.get(word);
					}
					collector_.emit(outputStream_, anchors, new Values(word, counts_.get(word)));
				} else {
					collector_.emit(outputStream_, new Values(word, counts_.get(word)));
				}
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
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		collector_ = collector;
		enableStormsTimeoutMechanism_ = context.enableStormDefaultTimeoutMechanism();
	}

	@Override
	public void execute(Tuple tuple) {
		
		if (enableStormsTimeoutMechanism_) {
			collector_.ack(tuple);
		}
		
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
		// this is used to push out the
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
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStream_, new Fields("word", "count"));
		declarer.declare(new Fields("word", "count"));
	}
}