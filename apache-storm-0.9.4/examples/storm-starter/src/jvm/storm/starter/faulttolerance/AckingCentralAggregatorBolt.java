package storm.starter.faulttolerance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class AckingCentralAggregatorBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private static final HashSet<Character> frequent_ = new HashSet<Character>(
			new ArrayList<Character>(Arrays.asList('a', 'b', 'c', 'd')));
	private static final HashSet<Character> moderatelyFequent_ = new HashSet<Character>(
			new ArrayList<Character>(Arrays.asList('e', 'f')));
	private static final HashSet<Character> lessFrequent_ = new HashSet<Character>(
			new ArrayList<Character>(Arrays.asList('g')));
	
	private Random rand_;
	
	public enum Delays {

		high(4000L), 
		moderate(2000L), 
		low(1000L);
		
		Long delay_;

		Delays(Long delay) {
			delay_ = delay;
		}
	};
	
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX_1 = 1;
	private static final int MESSAGE_INDEX_2 = 2;
	
	// output stream on which tuples will be emitted from this bolt
	private String outputStream_;

	// delay vs lastEmitTime for this delay bucket
	private HashMap<Long, Long> delayVsLastPushTime_ = new HashMap<Long, Long>();

	// delay vs Counts of words and its Tuples which are used to later
	// fail if ack is not received by PerEdgeAcking Storm
	// delay vs <word, count>
	private HashMap<Long, HashMap<String, Integer>> delayVsCounts_ = 
			new HashMap<Long, HashMap<String, Integer>>();

	// character vs List<anchors>
	private HashMap<String, List<Tuple>> wordVsAnchors_ = new HashMap<String, List<Tuple>>();

	public AckingCentralAggregatorBolt(String stream) {
		outputStream_ = stream;
	}
	
	public Long getDelayFor(String word) {
		
		if(word == null || word.isEmpty()) {
			return Delays.high.delay_;
		}
		
		char firstChar = word.charAt(0);
		if (frequent_.contains(firstChar)) {
			return Delays.low.delay_;
		} else if (moderatelyFequent_.contains(firstChar)) {
			return Delays.moderate.delay_;
		} else {
			return Delays.high.delay_;
		}
	}

	public void pushUpdates() {
		long now = System.currentTimeMillis();
		for (Long delay : delayVsLastPushTime_.keySet()) {
			
			boolean updatePushTime = false;
			// push updates only if last push time is more than delay
			if (now - delayVsLastPushTime_.get(delay) >= delay) {
				HashMap<String, Integer> counts = delayVsCounts_.get(delay);
				if(counts == null || counts.isEmpty()) {
					continue;
				}
				for (String word : counts.keySet()) {
					List<Tuple> anchors = wordVsAnchors_.containsKey(word) ? wordVsAnchors_.remove(word) : new ArrayList<Tuple>();
					emitTupleOnStream(anchors, new Values(word, counts.get(word)), outputStream_);
					updatePushTime = true;
				}
				// reset the counts after pushing
				if(updatePushTime) {
					delayVsCounts_.put(delay, new HashMap<String, Integer>());
					delayVsLastPushTime_.put(delay, now);
				}
			}
		}
		
		// reset the lastPush times
		Set<Long> delays = delayVsLastPushTime_.keySet();
		for(Long delay: delays) {
			delayVsLastPushTime_.put(delay, now);
		}
	}

	@Override
	public void customPrepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		delayVsLastPushTime_.put(Delays.high.delay_, System.currentTimeMillis());
		delayVsLastPushTime_.put(Delays.moderate.delay_, System.currentTimeMillis());
		delayVsLastPushTime_.put(Delays.low.delay_, System.currentTimeMillis());
		rand_ = new Random();
	}

	@Override
	public void customExecute(Tuple tuple) {

		Utils.sleep(Math.abs(rand_.nextInt() % 200));
		
		String word = tuple.getString(MESSAGE_INDEX_1);
		
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
		Long delay = getDelayFor(word);
		HashMap<String, Integer> curMap = delayVsCounts_.get(delay) == null ? 
				new HashMap<String, Integer>() : delayVsCounts_.get(delay);
		Integer count = tuple.getInteger(MESSAGE_INDEX_2);
		if(curMap.containsKey(word)) {
			count += curMap.get(word);
		}
		curMap.put(word, count);
		delayVsCounts_.put(delay, curMap);
		
		pushUpdates();
	}
				
	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStream_, new Fields("character", "count"));
		declarer.declare(new Fields("character", "count"));
	}
	
}