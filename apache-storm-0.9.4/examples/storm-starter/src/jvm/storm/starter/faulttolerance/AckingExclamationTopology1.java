package storm.starter.faulttolerance;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class AckingExclamationTopology1 {
	
	/**
	 * everything is same as {@link AckingExclamationTopology}, just that one
	 * bolt (bolt4) is not acking any tuples. This way we will test of our failure
	 * mechanism is working fine. 
	 * 
	 * TODO: RK TRY LATER
	 * 
	 * 1. for initial testing I have a parallelism of 1. once this works, we can
	 * go and increase the parallelism and try to make sure if things are
	 * working as expected
	 * 
	 * 2. I am only trying shuffleGrouping for now, need to take use cases for
	 * fieldsGrouping and all other grouping and test them as well.
	 * 
	 * 3. Using the Storm's default timeout mechanism for now, need to check
	 * this without using Storm's default timeout mechanism too.
	 * 
	 * 4. Using {@link TopologyBuilderExtraction} instead of
	 * {@link TopologyBuilder} Once {@link TopologyBuilderExtraction} and
	 * {@link TopologyBuilder} are merged, should use {@link TopologyBuilder}
	 * 
	 */
	private static String SPOUT = "word";
	private static String EXCLAIM_BOLT1 = "exclaim1";
	private static String EXCLAIM_BOLT2 = "exclaim2";
	private static String EXCLAIM_BOLT3 = "exclaim3";
	private static String EXCLAIM_BOLT4 = "exclaim4";
	private static String EXCLAIM_BOLT5 = "exclaim5";
	
	private static String SPOUT_SEND_STREAM = "SPOUT_SEND_STREAM";
	private static String B1_B2_SEND_STREAM = "B1_B2_SEND_STREAM";
	private static String B3_B4_SEND_STREAM = "B3_B4_SEND_STREAM";
	
	public static class AckingExclamationSpout extends BaseRichSpout {

		private static final long serialVersionUID = 1L;
		
		// RK NOTE: this should start the default storm ack tracker since we are
		// passing the tupleID as the message ID which will be used by storm as
		// a key to track the tuples progress.
		boolean _isDistributed;
	    SpoutOutputCollector _collector;
		Boolean enableStormsTimeoutMechanism_;

	    public AckingExclamationSpout() {
	        this(true);
	    }

	    public AckingExclamationSpout(boolean isDistributed) {
	        _isDistributed = isDistributed;
	    }
	        
	    @Override
	    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	        _collector = collector;
	        enableStormsTimeoutMechanism_ = context.enableStormDefaultTimeoutMechanism();
	    }

		@Override
	    public void nextTuple() {
	        Utils.sleep(100);
	        final String[] words = new String[] {"distributed", "computing", "systems", "group"};
	        final Random rand = new Random();
	        final String word = words[rand.nextInt(words.length)];
			// this is to make sure that we are always sending the TupleId in
			// tuple[0] and the actual message only starts from tuple[1]
	        String tupleId = new StringBuilder().append(new Random(Integer.MAX_VALUE).nextInt()).toString();
	        
	        Values vals = new Values(tupleId);
	        vals.add(word);

	        if(enableStormsTimeoutMechanism_) {
	        	//RKNOTE 
	        	// since we want Storm to track the tuples and its acks here
	        	// we need to give some messageId to emit (3rd argument).
	        	
	        	// But is this messageId needed in every bolt for acking?
	        	// or can we just not worry about it as long as the tuple is 
	        	// anchored and acked/failed?
	        	_collector.emit(SPOUT_SEND_STREAM, vals, tupleId);
	        } else {
	        	_collector.emit(SPOUT_SEND_STREAM, vals);
	        }
	    }
	  
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// spout_tupleId is needed to carry forward the rule that
			// tuple[0] should always have tupleId
			// we should probably add a new abstraction for AckingSpouts???
	        declarer.declareStream(SPOUT_SEND_STREAM, new Fields("spout_tupleId", "word"));
	    }

	    @Override
	    public Map<String, Object> getComponentConfiguration() {
	        if(!_isDistributed) {
	            Map<String, Object> ret = new HashMap<String, Object>();
	            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
	            return ret;
	        } else {
	            return null;
	        }
	    } 

	}
	
	public static void main(String[] args) throws Exception {
		
		AckingExclamationSpout spout = new AckingExclamationSpout();
		
		AbstractAckingBaseRichBolt bolt1 = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX = 1;
			// this is just to send messages to EXCLAIM_BOLT2 and EXCLAIM_BOLT3 alternately
			private Boolean sendToB2_ = new Boolean(true);
			
			@Override
			public void customPrepare(Map conf, TopologyContext context,
					OutputCollector collector) {
				sendToB2_ = new Boolean(true);
			}
			
			@Override
			public void customExecute(Tuple tuple) {
				// RK NOTE: I am assuming that getString(0) has the information
				// needed.
				// As Spout is sending directly to this bolt and it provides no
				// other fancy stuff other than the message
				// which is a simple string in this case.
				if(sendToB2_) {
					emitTupleOnStream(tuple, new Values(tuple.getString(MESSAGE_INDEX).concat("!B1!")), B1_B2_SEND_STREAM);
					sendToB2_ = !sendToB2_;
				} else {
					emitTuple(tuple, new Values(tuple.getString(MESSAGE_INDEX).concat("!B1!")));
				}
			}
			
			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declareStream(B1_B2_SEND_STREAM, new Fields("word"));
				declarer.declare(new Fields("word"));
			}
		};
		
		AbstractAckingBaseRichBolt bolt2 = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX = 1;
			
			@Override
			public void customExecute(Tuple tuple) {
				// I am assuming that getString(1) has the information
				// needed.
				// As bolt1 is sending to this bolt and it provides tupleId in
				// 0th index of tuple for per edge tracking.
				emitTuple(tuple, new Values(tuple.getString(MESSAGE_INDEX).concat("!B2!")));
			}

			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("word"));
			}
			
		};
		
		AbstractAckingBaseRichBolt bolt3 = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX = 1;
			
			@Override
			public void customExecute(Tuple tuple) {
				// RK NOTE: I am assuming that getString(1) has the information
				// needed.
				// As bolt1 is sending to this bolt and it provides tupleId in
				// 0th index of tuple for per edge tracking.
				emitTupleOnStream(tuple, new Values(tuple.getString(MESSAGE_INDEX).concat("!B3!")), B3_B4_SEND_STREAM);
			}
			
			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declareStream(B3_B4_SEND_STREAM, new Fields("word"));
				declarer.declare(new Fields("word"));
			}
		};
		
		AbstractAckingBaseRichBolt bolt4 = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX = 1;
						
			@Override
			public void customExecute(Tuple tuple) {
				// RK NOTE: I am assuming that getString(1) has the information
				// needed.
				// As bolt2 and bolt3 are sending to this bolt and it provides tupleId in
				// 0th index of tuple for per edge tracking.
				emitTuple(tuple, new Values(tuple.getString(MESSAGE_INDEX).concat("!B4!")));
			}

			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("word"));
			}
		};
		
		AbstractAckingBaseRichBolt bolt5 = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void customExecute(Tuple tuple) {
				// RK NOTE we just ack the tuples if enableStormDefaultTimeout_ here in @AckingBaseRichBolt
			}

			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("word"));
			}
			
		};
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT, spout, 1);
		builder.setBolt(EXCLAIM_BOLT1, bolt1, 1).shuffleGrouping(SPOUT, SPOUT_SEND_STREAM);
		builder.setBolt(EXCLAIM_BOLT2, bolt2, 1).shuffleGrouping(EXCLAIM_BOLT1, B1_B2_SEND_STREAM);
		builder.setBolt(EXCLAIM_BOLT3, bolt3, 1).shuffleGrouping(EXCLAIM_BOLT1);
		builder.setBolt(EXCLAIM_BOLT4, bolt4, 1).shuffleGrouping(EXCLAIM_BOLT3, B3_B4_SEND_STREAM).shuffleGrouping(EXCLAIM_BOLT2);
		builder.setBolt(EXCLAIM_BOLT5, bolt5, 1).shuffleGrouping(EXCLAIM_BOLT4);
		
		builder.addStreamTimeout(EXCLAIM_BOLT1, EXCLAIM_BOLT2, B1_B2_SEND_STREAM, 10000L)
				.addStreamTimeout(EXCLAIM_BOLT3, EXCLAIM_BOLT4, B3_B4_SEND_STREAM, 20000L);
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setDefaultPerEdgeTimeout(5000);
		conf.setUseStormTimeoutMechanism(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createTopology());
		}
	}

}
