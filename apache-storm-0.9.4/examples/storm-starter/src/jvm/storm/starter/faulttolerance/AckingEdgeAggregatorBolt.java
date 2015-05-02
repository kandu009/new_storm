package storm.starter.faulttolerance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.sun.istack.internal.logging.Logger;

public class AckingEdgeAggregatorBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private static final HashSet<Character> frequent_ = new HashSet<Character>(
			new ArrayList<Character>(Arrays.asList('a', 'b', 'c', 'd')));
	private static final HashSet<Character> moderatelyFequent_ = new HashSet<Character>(
			new ArrayList<Character>(Arrays.asList('e', 'f')));
	private static final HashSet<Character> lessFrequent_ = new HashSet<Character>(
			new ArrayList<Character>(Arrays.asList('g')));
	
	private static final Logger LOG = LoggerFactory.getLogger(AckingPrintBolt.class);
	
	public enum Delays {

		high(2000L), 
		moderate(1000L), 
		low(500L);
		
		Long delay_;

		Delays(Long delay) {
			delay_ = delay;
		}
	};
	
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX = 1;
	
	// output stream on which tuples will be emitted from this bolt
	private static String outputStream_;
	private Random _rand;
	
	// delay vs lastEmitTime for this delay bucket
	private HashMap<Long, Long> delayVsLastPushTime_ = new HashMap<Long, Long>();

	// delay vs Counts of words and its Tuples which are used to later
	// fail if ack is not received by PerEdgeAcking Storm
	// delay vs <word, count>
	private ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>> delayVsCounts_ = 
			new ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>>();

	// word vs List<anchors>
	private ConcurrentHashMap<String, List<Tuple>> wordVsAnchors_ = 
			new ConcurrentHashMap<String, List<Tuple>>();

	public AckingEdgeAggregatorBolt(String stream) {
		outputStream_ = stream;
	}
	
	public Long getDelayFor(String word) {
		
		if(word == null ||  word.isEmpty()) {
			return Delays.low.delay_;
		}
		
		char firstChar = word.charAt(0);
		if (frequent_.contains(firstChar)) {
			return Delays.high.delay_;
		} else if (moderatelyFequent_.contains(firstChar)) {
			return Delays.moderate.delay_;
		} else {
			return Delays.low.delay_;
		}
		
	}

	public void pushUpdates() {
		long now = System.currentTimeMillis();
		for (Long delay : delayVsLastPushTime_.keySet()) {
			// push updates only if last push time is more than delay
			if (now - delayVsLastPushTime_.get(delay) >= delay) {
				ConcurrentHashMap<String, Integer> counts = delayVsCounts_.get(delay);
				if(counts == null || counts.isEmpty()) {
					LOG.info("Not found any messsages in delayVsCounts_ for delay {" + delay +"}, so continuing !");
					continue;
				}
				for (String word : counts.keySet()) {
					List<Tuple> anchors = wordVsAnchors_.containsKey(word) ? wordVsAnchors_.remove(word) : new ArrayList<Tuple>();
					emitTupleOnStream(anchors, new Values(word, counts.get(word)), outputStream_);
				}
				// reset the counts after pushing
				delayVsCounts_.put(delay, new ConcurrentHashMap<String, Integer>());
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
		_rand = new Random();
	}

	@Override
	public void customExecute(Tuple tuple) {
		
		// this is to kind of achieve randomness as emitted by a
		// realistic source like twitter or some data feed
		Utils.sleep(Math.abs(_rand.nextInt() % 500));

		String word = tuple.getString(MESSAGE_INDEX);
		
		// we need to populate the anchors list belonging to this word
		// these anchors will later be used to fail the upstream if an
		// ack for this message is not received within timeout
		List<Tuple> l = new ArrayList<Tuple>(Arrays.asList(tuple)); 
		if(wordVsAnchors_.contains(word)) {
			l.addAll(wordVsAnchors_.get(word));
		}
		wordVsAnchors_.put(word, l);
		
		// updating the count of the word received in the
		// delayVsCounts_ structure. this is used to push out the
		// counts when the push was >= delay time, as done is
		// pushUpdates()
		Long delay = getDelayFor(word);
		ConcurrentHashMap<String, Integer> curMap = delayVsCounts_.get(delay) == null ? 
				new ConcurrentHashMap<String, Integer>() : delayVsCounts_.get(delay);
		Integer count = 1;
		if(curMap.contains(word)) {
			count += curMap.get(word);
		}
		curMap.put(word, count);
		delayVsCounts_.put(delay, curMap);
		
		StringBuilder sb = new StringBuilder();
		for(String w: curMap.keySet()) {
			sb.append(w).append(":").append(curMap.get(w)).append(",");
		}
		LOG.info("Pushing values to delayVsCounts_ for delay {" + delay +"}, and value {" + sb.toString() +"} !");
		
		pushUpdates();
	}

	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStream_, new Fields("word", "count"));
		declarer.declare(new Fields("word", "count"));
	}
}