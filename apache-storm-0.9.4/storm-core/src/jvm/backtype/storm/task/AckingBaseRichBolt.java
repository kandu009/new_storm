package backtype.storm.task;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.generated.Grouping;
import backtype.storm.testing.AckTracker;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContextConstants.Configuration;

public abstract class AckingBaseRichBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private static final Integer ROTATING_MAP_BUCKET_SIZE = 3;
	private static final String ACK_MESSAGE_DELIMITER = "_";
	private static final String TIMEOUTS_SEPARATOR = "|";
	private static final String ACK_STREAM_SEPARATOR = "|";
	private static final String TIMEOUT_DELIMITER = "#";
	private static final int ACK_MESSAGE_TOKEN_LENGTH = 5;
	private static final String ACK_MESSAGE_START_TOKEN = "ack_";

	private static final String TUPLE_ID_SEPARATOR = "_";
	
	//TODO: are all of these thread safe? we should be able to support a parallelism > 1
	private OutputCollector collector_;
	private String componentId_ = new String();
	private Random rand = new Random(Integer.MAX_VALUE);
	private long lastRotate_ = System.currentTimeMillis();
	private Long defaultPerEdgeTimeout_;
	private Boolean enableStormDefaultTimeout_;
	private TopologyContext context_;
	
	// since this is just constructed once and read everywhere else, it should
	// be fine even in case of multi threaded environment
	// This is the list of all the streams on which we are supposed to send
	// Ack's
	private HashSet<String> sendAckStream_ = new HashSet<String>();
	
	// TODO: not sure if the operations are all threadsafe, which is why I am using concurrent hashmap
	// <timeout vs <tupleId, OriginalTuple>> 
	private ConcurrentHashMap<Long, RotatingMap<String, Tuple>> ackTracker_ = new ConcurrentHashMap<Long, RotatingMap<String, Tuple>>();
	
	// since this is just constructed once and read everywhere else, it should
	// be fine even in case of multi threaded environment
	// This contains timeout information of all the streams that are sent from
	// this Bolt
	private HashMap<TimeoutIdentifier, Long> timeouts_ = new HashMap<TimeoutIdentifier, Long>();
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		collector_ = collector;
		componentId_ = context.getThisComponentId();
		defaultPerEdgeTimeout_ = context.getDefaultPerEdgeTimeout();
		enableStormDefaultTimeout_ = context.enableStormDefaultTimeoutMechanism();
		context_ = context;
		
		updateTimeouts(conf.get(Configuration.timeout.name()));
		updateAckStreams((String) conf.get(Configuration.send_ack.name()));
		
		createAckTrackersPerTimeout();
		
		customPrepare(conf, context, collector);
		
	}

	public abstract void customPrepare(Map conf, TopologyContext context, OutputCollector collector);

	private final String getTupleId(String sourceId, String targetId, String streamId) {
		return new StringBuilder().append(rand.nextInt())
				.append(TUPLE_ID_SEPARATOR).append(sourceId)
				.append(TUPLE_ID_SEPARATOR).append(targetId)
				.append(TUPLE_ID_SEPARATOR).append(streamId).toString();
	}
	
	private final String getTupleId(String sourceId, String targetId) {
		return getTupleId(sourceId, targetId, Utils.DEFAULT_STREAM_ID);
	}
	
	public final void execute(Tuple tuple) {
		
		checkForTimedOutTuples();
		
		if(tuple.getValue(0).toString().startsWith(ACK_MESSAGE_START_TOKEN)) {
			handleAckMessage(tuple);
			return;
		}
		
		sendAckMessage(tuple);

		customExecute(tuple);

		// If the user decides to use Storm's default timeout mechanism, then
		// ack the tuple in Storm way
		if(enableStormDefaultTimeout_) {
			collector_.ack(tuple);
		}
	}

	/**
	 * Sends ack messages to all the preceding components which are tracking the
	 * Tuples based on timeout
	 */
	private void sendAckMessage(Tuple tuple) {

		// we need to emit the ack only on that particular stream which is
		// responsible for sending this message.
		String ackingStreamId = componentId_ + ACK_MESSAGE_DELIMITER
				+ tuple.getSourceComponent() + ACK_MESSAGE_DELIMITER
				+ tuple.getSourceStreamId();

		// ack message will be like ack_tupleId_componentId_streamID
		// TODO: RK Note, here we are assuming that tuple.getValue(0) will have our
		// ack message which has our tupleID
		StringBuilder ackMsg = new StringBuilder().append(
				ACK_MESSAGE_START_TOKEN).append(tuple.getValue(0).toString());

		collector_.emit(ackingStreamId, new Values(ackMsg.toString()));
		
	}

	/**
	 * This method takes an Ack message and updates the {@link AckTracker}
	 * accordingly
	 */
	private void handleAckMessage(Tuple tuple) {
		
		// this will be of form ack_tupleId_componentId_streamID
		String ack = tuple.getValue(0).toString();
		String[] ackToks = ack.split(ACK_MESSAGE_DELIMITER);
		
		if(ACK_MESSAGE_TOKEN_LENGTH == ackToks.length) {
			String tupleKey = ack.substring(ack.indexOf("_")+1);
			findAndAckTuple(tupleKey);
		}
		
	}

	// this checks if there are any tuples which are not acked within the
	// specified timeout
	// and then explicitly fails them in Storm way falling back to Storm's
	// timeout/replay mechanism
	private void checkForTimedOutTuples() {
		
		// TODO: can this be done in a separate thread which runs for every 
		// min(perStreamTimeouts) seconds?
		for(Long timeout : ackTracker_.keySet()) {
			long now = System.currentTimeMillis();
			if(now - lastRotate_ > timeout) {
				Map<String, Tuple> failed = ackTracker_.get(timeout).rotate();
                for(String failedTuple : failed.keySet()) {
                	if(enableStormDefaultTimeout_) {
                		collector_.fail(failed.get(failedTuple));
                	} // else we can just ignore acking/failing tuples 
                }
			}
			lastRotate_ = now;
		}
	}

	/**
	 * This needs to be implemented by the specific user of this @AckingBaseRichBolt
	 */
	public abstract void customExecute(Tuple tuple);
	
	public void emitTupleOnStream(Tuple tuple, Values values, String streamId) {
		
		for(String targetId : getTargetsForStream(streamId)) {
			
			String tupleId = getTupleId(componentId_, targetId);
			if(enableStormDefaultTimeout_) {
				
				//TODO: check if Values object can be created like this
				
				// TODO RK NOTE: we are acking the tuples if enableStormDefaultTimeout_
				// is true in execute() method
				collector_.emit(streamId, tuple, new Values(tupleId, values));
			} else {
				//TODO: check if Values object can be created like this
				collector_.emit(streamId, new Values(tupleId, values));
			}

			TimeoutIdentifier ti = new TimeoutIdentifier(componentId_, targetId, streamId);
			Long timeout = timeouts_.containsKey(ti) ? timeouts_.get(ti) : defaultPerEdgeTimeout_; 
			ackTracker_.get(timeout).put(tupleId, tuple);
		}
		
	}
	
	public void emitTuple(Tuple tuple, Values values) {
		emitTupleOnStream(tuple, values, Utils.DEFAULT_STREAM_ID);
	}

	private HashSet<String> getTargetsForStream(String streamId) {
		Map<String, Map<String, Grouping>> targets = context_.getThisTargets();
		HashSet<String> ret = new HashSet<String>();
		if(targets.containsKey(streamId)) {
			ret.addAll(targets.get(streamId).keySet());
		}
		return ret;
	}
	
	public abstract void declareOutputFields(OutputFieldsDeclarer declarer);
	
	private void findAndAckTuple(String tupleKey) {
		for(Long at : ackTracker_.keySet()) {
			RotatingMap<String, Tuple> rmap = ackTracker_.get(at);
			if(rmap.containsKey(tupleKey)) {
				rmap.remove(tupleKey);
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
		for(TimeoutIdentifier i : timeouts_.keySet()) {
			if(!ackTracker_.containsKey(timeouts_.get(i))) {
				RotatingMap<String, Tuple> rmap = new RotatingMap<String, Tuple>(ROTATING_MAP_BUCKET_SIZE);
				ackTracker_.put(timeouts_.get(i), rmap);
			}
		}
		// we should also add a tracker for default per edge timeouts.
		ackTracker_.put(defaultPerEdgeTimeout_, new RotatingMap<String, Tuple>(ROTATING_MAP_BUCKET_SIZE));
	}
	
	private void updateTimeouts(Object timeouts) {
		
		if(timeouts == null) {
			return;
		}

		// all timeouts are of the form
		// key1#t1|key2#t2 ...
		// where key = sourceId+"_"+targetId+"_"+streamId;
		String[] timeoutsMap = ((String)timeouts).split(TIMEOUTS_SEPARATOR);
		for(String timeout : timeoutsMap) {
			String[] identifierTimeout = timeout.split(TIMEOUT_DELIMITER);
			if(identifierTimeout.length == 2) {
				try {
					timeouts_.put(new TimeoutIdentifier(identifierTimeout[0]), Long.parseLong(identifierTimeout[1]));
				} catch (NumberFormatException e) {
				} catch (UnrecognizedTimeoutIdentifier e) {
				}
			}
		}
	}
	
	private void updateAckStreams(String ackStreams) {
		// all ack streams are of the form
		// key1 | key2 | ....
		// where key = targetId+"_"+boltId+"_"+streamId
		String[] ackStreamArray = ackStreams.split(ACK_STREAM_SEPARATOR);
		for(String ackStream : ackStreamArray) {
			if(!ackStream.trim().isEmpty()) {
				sendAckStream_.add(ackStream.trim());
			}
		}
	}
	
}
