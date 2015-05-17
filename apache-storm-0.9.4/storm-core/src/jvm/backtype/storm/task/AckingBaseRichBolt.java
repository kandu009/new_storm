package backtype.storm.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StreamInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.ShellSpout;
import backtype.storm.testing.AckTracker;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;
import backtype.storm.task.TopologyContextConstants.Configuration;

public abstract class AckingBaseRichBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	public static Logger LOG = LoggerFactory.getLogger(ShellSpout.class);

	private static final Integer ROTATING_MAP_BUCKET_SIZE = 3;
	private static final String ACK_MESSAGE_DELIMITER = "_";
	private static final String TIMEOUTS_SEPARATOR = "|";
	private static final String ACK_STREAM_SEPARATOR = "|";
	private static final String TIMEOUT_DELIMITER = "_";
	private static final int ACK_MESSAGE_TOKEN_LENGTH = 2;
	private static final String ACK_MESSAGE_START_TOKEN = "ack_";

	private static final String TUPLE_ID_SEPARATOR = "_";
	private static final int ACTUAL_MESSAGE_INDEX = 1;
	private static final int TUPLE_ID_INDEX = 0;
	private static String TUPLE_ID_FIELD_NAME = "_tupleId";
	
	private OutputCollector collector_;
	private String componentId_ = new String();
	private Random rand = new Random(Integer.MAX_VALUE);
	private Long defaultPerEdgeTimeout_;
	private Boolean enableStormDefaultTimeout_;
	private TopologyContext context_;
	
	// since this is just constructed once and read everywhere else, it should
	// be fine even in case of multi threaded environment
	// This is the list of all the streams on which we are supposed to send
	// Ack's
	private HashSet<String> sendAckStream_ = new HashSet<String>();
	
	// <timeout vs <tupleId, OriginalTuple>> 
	private HashMap<Long, RotatingMap<String, List<Tuple>>> ackTracker_ = new HashMap<Long, RotatingMap<String, List<Tuple>>>();
	// this is used to keep the last rotated time of each ack Tracker (with a corresponding timeout)
	private HashMap<Long, Long> ackTrackerVsLastRotate_ = new HashMap<Long, Long>();
	
	// since this is just constructed once and read everywhere else, it should
	// be fine even in case of multi threaded environment
	// This contains timeout information of all the streams that are sent from
	// this Bolt
	private HashMap<TimeoutIdentifier, Long> timeouts_ = new HashMap<TimeoutIdentifier, Long>();
	
	public final void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		collector_ = collector;
		componentId_ = context.getThisComponentId();
		defaultPerEdgeTimeout_ = context.getDefaultPerEdgeTimeout();
		enableStormDefaultTimeout_ = context.enableStormDefaultTimeoutMechanism();
		context_ = context;
		
		updateTimeouts(conf.get(Configuration.timeout.name()));
		updateAckStreams(conf.get(Configuration.send_ack.name()));
		
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

		// If the user decides to use Storm's default timeout mechanism, then
		// ack the tuple in Storm way
		LOG.info("Received tuple {" + tuple.getString(0) + "} in task {" + context_.getThisTaskId() + "}");
		if(enableStormDefaultTimeout_) {
			// RK NOTE: adding this only to check if failures are correctly
			// identified by default storm topology 
			LOG.info("Storm acking {" + tuple.getString(0) + "} in task {" + context_.getThisTaskId() + "}");
			collector_.ack(tuple);
		}
		
		if(tuple.getValue(ACTUAL_MESSAGE_INDEX).toString().startsWith(ACK_MESSAGE_START_TOKEN)) {
			handleAckMessage(tuple);
			return;
		}
		
		sendAckMessage(tuple);

		customExecute(tuple);

		checkForTimedOutTuples();
		
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
		// RK NOTE: here we are assuming that tuple.getValue(0) will have our
		// ack message which has our tupleID
		StringBuilder ackMsg = new StringBuilder().append(
				ACK_MESSAGE_START_TOKEN).append(tuple.getValue(TUPLE_ID_INDEX).toString());

		// this is to make sure that all messages that we send from an
				// AckingBolt has the first tuple field as tupleId
		String tupleId = getTupleId(componentId_, tuple.getSourceComponent(), ackingStreamId);
		Values vals = new Values(tupleId);
		vals.add(ackMsg.toString());
		collector_.emitDirect(tuple.getSourceTask(), ackingStreamId, vals);
		LOG.info("Sending an ack message for the tuple with ID {" + tuple.getValue(TUPLE_ID_INDEX).toString() +
				"} on stream {" + ackingStreamId +"} to taskID {" + tuple.getSourceTask() +"}");
	}

	/**
	 * This method takes an Ack message and updates the {@link AckTracker}
	 * accordingly
	 */
	private void handleAckMessage(Tuple tuple) {
		// this will be of form ack_tupleId_componentId_streamID
		// we are assuming that the tuple[0] will have the tuple ID, 
		// we will not use tupleId for acking messages. 
		String ack = tuple.getValue(ACTUAL_MESSAGE_INDEX).toString();
		String[] ackToks = ack.split("[*"+ACK_MESSAGE_DELIMITER+"*]+");
		
		LOG.info("HandleAckMessage {" + ack + "} in task {" + context_.getThisTaskId() + "}");
		
		if(ACK_MESSAGE_TOKEN_LENGTH <= ackToks.length) {
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
		LOG.debug("Checking for timed out tuples !");
		
		for(Long timeout : ackTracker_.keySet()) {
			
			long now = System.currentTimeMillis();
			long lastRotate_ = ackTrackerVsLastRotate_.get(timeout);
			
			if(now - lastRotate_ > timeout) {
				Map<String, List<Tuple>> failed = ackTracker_.get(timeout).rotate();
				if(failed.isEmpty()) {
					LOG.info("No failed Tuples in this Bucket !!!");
				}
                for(String failedTuple : failed.keySet()) {
                	LOG.error("Tuple {" + failedTuple + "} has failed to get an acknowledgement on time on taskID {" + context_.getThisTaskId() + "} !!!");
                	if(enableStormDefaultTimeout_) {
                		for(Tuple t : failed.get(failedTuple)) {
                			collector_.fail(t);
                		}
                	} // else we can just ignore acking/failing tuples 
                }
                ackTrackerVsLastRotate_.put(timeout, System.currentTimeMillis());
                LOG.debug("Updating lastRotate of {" + timeout + "} to {"+ ackTrackerVsLastRotate_.get(timeout) + "}");
			} else {
				LOG.debug("Last rotate wasn't too long for {" + timeout + "} !");
			}
			
		}
	}

	/**
	 * This needs to be implemented by the specific user of this @AckingBaseRichBolt
	 */
	public abstract void customExecute(Tuple tuple);
	
	public void emitTupleOnStream(List<Tuple> anchors, Values values, String streamId) {
		
		for(String targetId : getTargetsForStream(streamId)) {
			String tupleId = getTupleId(componentId_, targetId, streamId);
			Values newVals = new Values(tupleId);
			newVals.addAll(values);
			
			StringBuilder srcTupleId = new StringBuilder();
			if(anchors != null && !anchors.isEmpty()) {
				for(Tuple t : anchors) {
					srcTupleId.append(t.getString(0)).append(",");	// this is to track which source tuple is tied to which target tuple
				}
			}
			// RK NOTE: we are acking the tuples if enableStormDefaultTimeout_
			if(enableStormDefaultTimeout_) {
				// is true in execute() method
				collector_.emit(streamId, anchors, newVals);
				LOG.info("Emitting tuple {" + tupleId + "} on {" + streamId
						+ "} from task {" + context_.getThisTaskId()
						+ "}, source tuple ID's {" + srcTupleId.toString()+ "}");
			} else {
				LOG.info("Emitting tuple {" + tupleId + "} on {" + streamId
						+ "} from task {" + context_.getThisTaskId()
						+ "}, source tuple ID's {" + srcTupleId.toString()+ "}");
				collector_.emit(streamId, newVals);
			}

			TimeoutIdentifier ti = new TimeoutIdentifier(componentId_, targetId, streamId);
			Long timeout = timeouts_.containsKey(ti) ? timeouts_.get(ti) : defaultPerEdgeTimeout_;
			RotatingMap<String, List<Tuple>> rmap = ackTracker_.get(timeout);
			rmap.put(tupleId, anchors);
			ackTracker_.put(timeout, rmap);
			LOG.info("Adding tuple {" + tupleId
					+ "} to the tracker with sourceTuples {" + srcTupleId
					+ "} timeout Identifier {" + componentId_ + "," + targetId
					+ "," + streamId + "} and timeout {" + timeout + "}");
		}
		
	}
	
	public void emitTuple(Tuple anchor, Values values) {
		emitTupleOnStream(new ArrayList<Tuple>(Arrays.asList(anchor)), values, Utils.DEFAULT_STREAM_ID);
	}
	
	public void emitTupleOnStream(Tuple anchor, Values values, String streamId) {
		emitTupleOnStream(new ArrayList<Tuple>(Arrays.asList(anchor)), values, streamId);
	}
	
	public void emitTuple(List<Tuple> anchors, Values values) {
		emitTupleOnStream(anchors, values, Utils.DEFAULT_STREAM_ID);
	}
	
	private HashSet<String> getTargetsForStream(String streamId) {
		Map<String, Map<String, Grouping>> targets = context_.getThisTargets();
		HashSet<String> ret = new HashSet<String>();
		if(targets.containsKey(streamId)) {
			ret.addAll(targets.get(streamId).keySet());
		}
		return ret;
	}
	
	public final void declareOutputFields(OutputFieldsDeclarer declarer) {
		AckingOutputFieldsDeclarer tempDeclarer = new AckingOutputFieldsDeclarer();
		customDeclareOutputFields(tempDeclarer);
		
		Map<String, StreamInfo> fieldsMap = tempDeclarer.getFieldsDeclaration();
		for(String strId : fieldsMap.keySet()) {
			// we are adding a tupleId field for all the streamIds that are
			// possibly used by this component.
			List<String> newFields = fieldsMap.get(strId).get_output_fields();
			newFields.add(TUPLE_ID_FIELD_NAME);
			LOG.debug("Adding a custom field {" + TUPLE_ID_FIELD_NAME + "} to the stream {" + strId + "}");
			declarer.declareStream(strId, fieldsMap.get(strId).is_direct(), new Fields(newFields));
		}
	}
	
	public abstract void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer);
	
	private void findAndAckTuple(String tupleKey) {
		LOG.info("findAndAckTuple {" + tupleKey + "} in task {" + context_.getThisTaskId() + "}");
		for(Long at : ackTracker_.keySet()) {
			RotatingMap<String, List<Tuple>> rmap = ackTracker_.get(at);
			if(rmap.containsKey(tupleKey)) {
				LOG.info("Acking Tuple with key {" + tupleKey + "} in taskId {" + context_.getThisTaskId() + "}");
				rmap.remove(tupleKey);
				ackTracker_.put(at, rmap);
			} else {
				LOG.info("Tuple {" + tupleKey + "} is not present in {" + at + "} in taskId {" + context_.getThisTaskId() + "}");
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
				RotatingMap<String, List<Tuple>> rmap = new RotatingMap<String, List<Tuple>>(ROTATING_MAP_BUCKET_SIZE);
				ackTracker_.put(timeouts_.get(i), rmap);
				LOG.info("Created an Ack Tracker with timeout {" + timeouts_.get(i) + "}");
				ackTrackerVsLastRotate_.put(timeouts_.get(i), System.currentTimeMillis());
				LOG.info("Added an entry for timeout { " + timeouts_.get(i) + "} -> {" + ackTrackerVsLastRotate_.get(timeouts_.get(i)) + "} in LastRotate dataset");
			}
		}
		
		// we should also add a tracker for default per edge timeouts.
		ackTracker_.put(defaultPerEdgeTimeout_, new RotatingMap<String, List<Tuple>>(ROTATING_MAP_BUCKET_SIZE));
		LOG.info("Created an Ack Tracker with default timeout {" + defaultPerEdgeTimeout_ + "}");
		ackTrackerVsLastRotate_.put(defaultPerEdgeTimeout_, System.currentTimeMillis());
		LOG.info("Added an entry for default timeout { " + defaultPerEdgeTimeout_ + "} in LastRotate dataset");
	}
	
	private void updateTimeouts(Object timeouts) {
		if(timeouts == null) {
			LOG.info("There are no additional timeouts specified, will use the default per edge timeout !!!");
			return;
		}

		// all timeouts are of the form
		// key1_t1|key2_t2 ...
		// where key = sourceId+"_"+targetId+"_"+streamId;
		String[] timeoutsMap = ((String)timeouts).split("[*"+TIMEOUTS_SEPARATOR+"*]+");
		
		for(int i = 0; i < timeoutsMap.length; ++i) {
		
			String[] timeoutToks = timeoutsMap[i].split("[*"+TIMEOUT_DELIMITER+"*]+");
			
			if(timeoutToks.length >= 4) {
				try {
					StringBuilder streamId = new StringBuilder();
					//TODO: I did not realize that having '_' in a streamId would lead to all these issues,
					//need to comeup with different separators for different stuff
					int k = 2;
					while(k < timeoutToks.length-1) {
						streamId.append(timeoutToks[k]).append("_");
						++k;
					}
					if(!streamId.toString().isEmpty()) {
						streamId.deleteCharAt(streamId.lastIndexOf("_"));
					}
					
					timeouts_.put(new TimeoutIdentifier(timeoutToks[0],
							timeoutToks[1], streamId.toString()), 
							Long.parseLong(timeoutToks[timeoutToks.length-1]));
					
					LOG.info("Adding new per edge timeout {"
							+ timeoutToks[timeoutToks.length - 1]
							+ "} with key {" + timeoutToks[0] + ", "
							+ timeoutToks[1] + ", " + streamId.toString() + "}");
				} catch (Exception e) {
				
				}
			}
			
		}
	}
	
	private void updateAckStreams(Object ackStreams) {
		if(ackStreams == null) {
			LOG.info("There are no Ack Streams, we should be good !!!");
			return;
		}
		// all ack streams are of the form
		// key1 | key2 | ....
		// where key = targetId+"_"+boltId+"_"+streamId
		String[] ackStreamArray = ((String)ackStreams).split("[*"+ACK_STREAM_SEPARATOR+"*]+");
		for(int i = 0; i < ackStreamArray.length; ++i) {
			if(!ackStreamArray[i].trim().isEmpty()) {
				sendAckStream_.add(ackStreamArray[i].trim());
				LOG.info("Adding Ack Stream {" + ackStreamArray[i].trim() +"}");
			}
		}
	}
	
	public int getThisTaskId() {
		return context_.getThisTaskId();
	}
    
}
