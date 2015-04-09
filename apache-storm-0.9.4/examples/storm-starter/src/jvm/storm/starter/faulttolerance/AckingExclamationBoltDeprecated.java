package storm.starter.faulttolerance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RotatingMap;
import backtype.storm.task.TopologyContextConstants.Configuration;

@Deprecated
// this was initially written to get an idea of what all actions 
// needs to be performed for supporting per edge timeout mechanism
public class AckingExclamationBoltDeprecated extends AckingBaseRichBolt {

	public enum SendReceiveToken {
		send_msg,
		receive_msg,
		receive_ack;
	}
	
	private static final long serialVersionUID = 1L;

	private static final Integer ROTATING_MAP_BUCKET_SIZE = 3;

	private static final String ACK_MESSAGE_DELIMITER = "_";

	private static final int ACK_MESSAGE_TOKEN_LENGTH = 3;
	
	private static final int DEFAULT_TIMEOUT = 1000;
	
	//TODO: are all of these thread safe? because if we have a parallelism > 1, then we might also
	// want to see if all of these operations are thread safe too !!!
	OutputCollector collector_;
	String componentId_ = new String();
	Integer timeout_ = -1;
	
	// TODO: these are the ones that we have manually picked up from the configuration.
	// These are for managing sending and receiving messages
	HashMap<String, Long> sendStreamVsTimeout_ = new HashMap<String, Long>();
	HashSet<String> receiveStream_ = new HashSet<String>();
	Random rand = new Random(Integer.MAX_VALUE);
	
	// These are for managing the acknowledgement messages
	HashSet<String> sendAckStream_ = new HashSet<String>();
	HashSet<String> receiveAckStream_ = new HashSet<String>();
	
	// TODO: looks like the streamID vs componentID can be picked up from TopologyContext directly
	// trying that out, if things work well we can just ignore the others.
	// this holds info like Map <streamID, Map <componentID, grouping> >
	// private Map<String, Map<String, Grouping>> targetsInfo_; 
	
	// TODO: should this be a concurrent hashmap?
	HashMap<Long, RotatingMap<String, Tuple>> ackTracker_ = new HashMap<Long, RotatingMap<String, Tuple>>();
	
	long lastRotate_ = System.currentTimeMillis();
	
	public AckingExclamationBoltDeprecated(Integer timeout) {
		timeout_ = timeout;
	}
	
	public AckingExclamationBoltDeprecated() {
		this(DEFAULT_TIMEOUT);
	}
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		sendStreamVsTimeout_.putAll(getStreamIdTimeoutMapping((ArrayList<SendStreamTimeoutPair>) conf.get(SendReceiveToken.send_msg.name())));
		receiveStream_.addAll((ArrayList<String>) conf.get(SendReceiveToken.receive_msg.name()));
		
		sendAckStream_.addAll((ArrayList<String>) conf.get(Configuration.send_ack.name()));
		receiveAckStream_.addAll((ArrayList<String>) conf.get(SendReceiveToken.receive_ack.name()));
		
		componentId_ = context.getThisComponentId();
		
		//TODO : we might need this when we modify submitTopology/TopologyBuilder for automatically 
		// finding the components and giving the streamID's on the fly 
		//targetsInfo_ = context.getTargets(componentId_);
		
		collector_ = collector;
		createAckTrackersPerTimeout();
		
	}

	/*public void execute(Tuple tuple) {
		
		//TODO: can this be done in a separate thread which runs for every min(perStreamTimeouts) seconds?
		for(Long timeout : ackTracker_.keySet()) {
			long now = System.currentTimeMillis();
			if(now - lastRotate_ > timeout) {
				Map<String, Tuple> failed = ackTracker_.get(timeout).rotate();
                for(String failedTuple : failed.keySet()) {
                	collector_.fail(failed.get(failedTuple));
                }
			}
			lastRotate_ = now;
		}
		
		if(tuple.getValue(0).toString().startsWith("ack_")) {
			// this will be of form ack_tupleId_componentId
			String ack = tuple.getValue(0).toString();
			String[] ackToks = ack.split(ACK_MESSAGE_DELIMITER);
			if(ACK_MESSAGE_TOKEN_LENGTH == ackToks.length) {
				String tid = ackToks[1];
				findAndAckTuple(tid);
			}
			// TODO: use the tupleID for marking them as ack'ed. Not sure if componentId is actually needed, as every tuple emitted is a unique tuple per stream ID.
			return;
		}
		
		// ack message will be like ack_tupleId_componentId
		String ackMsg = new String().concat("ack_").concat(tuple.getValue(0).toString()).concat("_").concat(componentId_);
		for(String a : sendAckStream_) {
			collector_.emit(a, new Values(ackMsg));
		}

		// send the new tuple on every output stream now and update the tracker +
		// the timer which keeps a track of whether the tuples are ack'ed within timeout or not 
		for(String s : sendStreamVsTimeout_.keySet()) {
			//TODO: we are assuming that the tuple[0] will contain tupleID, this may not be the
			// case when the tuple is emitted by a Spout. How can this be solved?
			String tupleId = tuple.getString(0).concat(new Integer(rand.nextInt()).toString());
			collector_.emit(s, tuple, new Values(tupleId, tuple.getString(1) + "!!!"));
			ackTracker_.get(sendStreamVsTimeout_.get(s)).put(tupleId, tuple);;
		}
		collector_.emit(new Values(tuple.getString(1)  + "!!!"));
		
		// TODO: I think this should not be done as we want to fall back back to 
		// storm's default timeout/fault-tolerance mechanism 
		// _collector.ack(tuple);
	}*/

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	private void findAndAckTuple(String tupleId) {
		for(Long at : ackTracker_.keySet()) {
			RotatingMap<String, Tuple> rmap = ackTracker_.get(at);
			if(rmap.containsKey(tupleId)) {
				Tuple tuple = (Tuple) rmap.remove(tupleId);
				collector_.ack(tuple);
			}
		}
	}
	
	/**
	 * We know the total number of different possible timeouts for this
	 * component (including all the streams) in prepare method, we can use this
	 * information to dynamically create a set of {@link RotatingMap} instances
	 * which correspond to each of these timeouts.
	 */
	private void createAckTrackersPerTimeout() {
		for(String s : sendStreamVsTimeout_.keySet()) {
			if(!ackTracker_.containsKey(sendStreamVsTimeout_.get(s))) {
				RotatingMap<String, Tuple> rmap = new RotatingMap<String, Tuple>(ROTATING_MAP_BUCKET_SIZE);
				ackTracker_.put(sendStreamVsTimeout_.get(s), rmap);
			}
		}
	}
	
	private HashMap<String, Long> getStreamIdTimeoutMapping(ArrayList<SendStreamTimeoutPair> sIdTimeouts) {
		if(sIdTimeouts != null && !sIdTimeouts.isEmpty()) {
			for(SendStreamTimeoutPair st : sIdTimeouts) {
				sendStreamVsTimeout_.put(st.getStreamId(), st.getTimeout());
			}
		}
		return sendStreamVsTimeout_;
	}

	@Override
	public void customExecute(Tuple tuple) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void customPrepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

}