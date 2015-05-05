package storm.starter.faulttolerance;

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

public class SimpleAckingCentralAggregatorBolt extends AbstractAckingBaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX_1 = 1;
	private static final int MESSAGE_INDEX_2 = 2;
	
	// output stream on which tuples will be emitted from this bolt
	private String outputStream_;
	private static long WINDOW_LENGTH = 4000L;

	// lastEmitTime
	private Long lastPushTime_ = System.currentTimeMillis();

	// delay vs Counts of characters and its Tuples which are used to later
	// fail if ack is not received by PerEdgeAcking Storm
	// delay vs <character, count>
	private HashMap<String, Integer> counts_ = new HashMap<String, Integer>();

	// character vs List<anchors>
	private HashMap<String, List<Tuple>> charVsAnchors_ = new HashMap<String, List<Tuple>>();

	public SimpleAckingCentralAggregatorBolt(String stream) {
		outputStream_ = stream;
	}
	
	public void pushUpdates() {
		long now = System.currentTimeMillis();
		boolean updatePushTime = false;
		// push updates only if last push time is more than delay
		if (now - lastPushTime_ >= WINDOW_LENGTH) {
			for (String character : counts_.keySet()) {
				List<Tuple> anchors = new ArrayList<Tuple>();
				if(charVsAnchors_.containsKey(character)) {
					anchors = charVsAnchors_.get(character);
				}
				emitTupleOnStream(anchors, new Values(character, counts_.get(character)), outputStream_);
				updatePushTime = true;
				charVsAnchors_.remove(character);
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

		Utils.sleep(1000);
		
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
		Integer count = tuple.getInteger(MESSAGE_INDEX_2);
		if(counts_.containsKey(character)) {
			count += counts_.get(character);
		}
		counts_.put(character, count);
		
		pushUpdates();
	}
				
	@Override
	public void customDeclareOutputFields(
			AckingOutputFieldsDeclarer declarer) {
		declarer.declareStream(outputStream_, new Fields("character", "count"));
		declarer.declare(new Fields("character", "count"));
	}
	
}