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

	// delay vs Counts of characters and its Tuples which are used to later
	// fail if ack is not received by PerEdgeAcking Storm
	// delay vs <character, count>
	private HashMap<Long, HashMap<String, Integer>> delayVsCounts_ = 
			new HashMap<Long, HashMap<String, Integer>>();

	// character vs List<anchors>
	private HashMap<String, List<Tuple>> charVsAnchors_ = new HashMap<String, List<Tuple>>();

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
				for (String character : counts.keySet()) {
					List<Tuple> anchors = charVsAnchors_.containsKey(character) ? charVsAnchors_.remove(character) : new ArrayList<Tuple>();
					emitTupleOnStream(anchors, new Values(character, counts.get(character)), outputStream_);
					updatePushTime = true;
				}
				// reset the counts after pushing
				if(updatePushTime) {
					delayVsCounts_.put(delay, new HashMap<String, Integer>());
					delayVsLastPushTime_.put(delay, now);
				}
			}
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

		Utils.sleep(Math.abs(rand_.nextInt()) % 200);
		
		String character = new StringBuilder().append(tuple.getString(MESSAGE_INDEX_1).charAt(0)).toString(); 
		
		// we need to populate the anchors list belonging to this character
		// these anchors will later be used to fail the upstream if an
		// ack for this message is not received within timeout
		List<Tuple> l = new ArrayList<Tuple>(Arrays.asList(tuple)); 
		if(charVsAnchors_.containsKey(character)) {
			l.addAll(charVsAnchors_.get(character));
		}
		charVsAnchors_.put(character, l);
		
		// updating the count of the character received in the
		// delayVsCounts_ structure. this is used to push out the
		// counts when the push was >= delay time, as done is
		// pushUpdates()
		Long delay = getDelayFor(character);
		HashMap<String, Integer> curMap = delayVsCounts_.get(delay) == null ? 
				new HashMap<String, Integer>() : delayVsCounts_.get(delay);
		Integer count = tuple.getInteger(MESSAGE_INDEX_2);
		if(curMap.containsKey(character)) {
			count += curMap.get(character);
		}
		curMap.put(character, count);
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