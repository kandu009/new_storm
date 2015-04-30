package storm.starter.faulttolerance;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * 
 * @author rkandur
 *
 *         another use case of Per Edge Acking topology. using this to conduct
 *         experiments This is more easier to realize as a topology which can
 *         have intentional delays.
 *         
 */
public class AckingWordCountTopology1 {
	
	private static final String SPOUT_SEND_STREAM = "spoutSendStream";
	private static final String SPLITTER_AGGREGATOR_SEND_STREAM = "splitterAggregatorStream";
	private static final String AGGREGATOR_SUPERAGGREGATOR_SEND_STREAM = "aggregatorSuperaggregatorStream";
	private static final String SUPERAGGREGATOR_PRINT_SEND_STREAM = "superAggregatorPrintStream";
		
	private static final String SENTENCE_SPOUT = "sentencespout";
	private static final String SPLITER_BOLT = "splitterBolt";
	private static final String AGGREGATOR_BOLT = "aggregatorBolt";
	private static final String SUPERAGGREGATOR_BOLT = "superAggregatorBolt";
	private static final String PRINTER_BOLT = "printerBolt";
	
	private static final Logger LOG = LoggerFactory.getLogger(AckingWordCountTopology1.class);
	
	public static class AckingRandomSentenceSpout extends BaseRichSpout {
		
		private static final long serialVersionUID = 1L;

		// RK NOTE: this should start the default storm ack tracker since we are
		// passing the tupleID as the message ID which will be used by storm as
		// a key to track the tuples progress.
		SpoutOutputCollector _collector;
		Random _rand;
		Boolean enableStormsTimeoutMechanism_;

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			_collector = collector;
			_rand = new Random();
			enableStormsTimeoutMechanism_ = context.enableStormDefaultTimeoutMechanism();
		}

		@Override
		public void nextTuple() {
			Utils.sleep(100);
			final Random rand = new Random();
			String[] sentences = new String[] { "the cow jumped over the moon",
					"an apple a day keeps the doctor away",
					"four score and seven years ago",
					"snow white and the seven dwarfs",
					"i am at two with nature" };
			String sentence = sentences[_rand.nextInt(sentences.length)];
			String tupleId = new StringBuilder().append(new Random(Integer.MAX_VALUE).nextInt()).toString();

			Values vals = new Values(tupleId);
			vals.add(sentence);

			if (enableStormsTimeoutMechanism_) {
				// RKNOTE
				// since we want Storm to track the tuples and its acks here
				// we need to give some messageId to emit (3rd argument).

				// But is this messageId needed in every bolt for acking?
				// or can we just not worry about it as long as the tuple is
				// anchored and acked/failed?
				_collector.emit(SPOUT_SEND_STREAM, vals, tupleId);
			} else {
				_collector.emit(SPOUT_SEND_STREAM, vals);
			}

			_collector.emit(new Values(sentence));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// spout_tupleId is needed to carry forward the rule that
			// tuple[0] should always have tupleId
			// we should probably add a new abstraction for AckingSpouts???
	        declarer.declareStream(SPOUT_SEND_STREAM, new Fields("spoutTupleId", "word"));
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

			AckingRandomSentenceSpout spout = new AckingRandomSentenceSpout();
			
			AbstractAckingBaseRichBolt sentenceSplitBolt = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX = 1;
			
			@Override
			public void customPrepare(Map conf, TopologyContext context,
					OutputCollector collector) {
			}
			
			@Override
			public void customExecute(Tuple tuple) {
				// As Spout is sending directly to this bolt and it provides no
				// other fancy stuff other than the message
				// which is a simple string in this case.
				String[] words = tuple.getString(MESSAGE_INDEX).split("[* *]+");
				for(int i = 0; i < words.length; ++i) {
					emitTupleOnStream(tuple, new Values(words[i]), SPLITTER_AGGREGATOR_SEND_STREAM);
				}
			}
			
			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declareStream(SPLITTER_AGGREGATOR_SEND_STREAM, new Fields("word"));
				declarer.declare(new Fields("word"));
			}
		};
		
		AbstractAckingBaseRichBolt aggregatorBolt = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX = 1;
			// data structure to hold the counts of words
			private HashMap<String, Integer> counts = new HashMap<String, Integer>();

			@Override
			public void customPrepare(Map conf, TopologyContext context,
					OutputCollector collector) {
			}
			
			@Override
			public void customExecute(Tuple tuple) {
				// As Spout is sending directly to this bolt and it provides no
				// other fancy stuff other than the message
				// which is a simple string in this case.
				String word = tuple.getString(MESSAGE_INDEX);
				Integer count = 1;
			    if(counts.containsKey(word)) {
			    	count += counts.get(word);
			    } 
			    counts.put(word, count);
			    emitTupleOnStream(tuple, new Values(word, count), AGGREGATOR_SUPERAGGREGATOR_SEND_STREAM);
			}
			
			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declareStream(AGGREGATOR_SUPERAGGREGATOR_SEND_STREAM, new Fields("word", "count"));
				declarer.declare(new Fields("word", "count"));
			}
		};
		
		AbstractAckingBaseRichBolt superAggregatorBolt = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX_1 = 1;
			private static final int MESSAGE_INDEX_2 = 2;
			
			// data structure to hold the counts of words
			private HashMap<Character, Integer> counts_;

			@Override
			public void customPrepare(Map conf, TopologyContext context,
					OutputCollector collector) {
				counts_ = new HashMap<Character, Integer>();
			}
			
			@Override
			public void customExecute(Tuple tuple) {
				// As Spout is sending directly to this bolt and it provides no
				// other fancy stuff other than the message
				// which is a simple string in this case.
				Character c = tuple.getString(MESSAGE_INDEX_1).charAt(0);
				Integer count = tuple.getInteger(MESSAGE_INDEX_2);
			    if(counts_.containsKey(c)) {
			    	count += counts_.get(c);
			    } 
			    
			    counts_.put(c, count);
			    emitTupleOnStream(tuple, new Values(c.toString(), count), SUPERAGGREGATOR_PRINT_SEND_STREAM);
			}
			
			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
				declarer.declareStream(SUPERAGGREGATOR_PRINT_SEND_STREAM, new Fields("character", "count"));
				declarer.declare(new Fields("character", "count"));
			}
		};

		AbstractAckingBaseRichBolt printBolt = new AbstractAckingBaseRichBolt() {
			
			private static final long serialVersionUID = 1L;
			// this just gives you index in tuple which holds the incoming message
			private static final int MESSAGE_INDEX_1 = 1;
			private static final int MESSAGE_INDEX_2 = 2;
			
			@Override
			public void customPrepare(Map conf, TopologyContext context,
					OutputCollector collector) {
			}
			
			@Override
			public void customExecute(Tuple tuple) {
				// As Spout is sending directly to this bolt and it provides no
				// other fancy stuff other than the message
				// which is a simple string in this case.
				String word = tuple.getString(MESSAGE_INDEX_1);
				Integer count = tuple.getInteger(MESSAGE_INDEX_2);
				LOG.info("Alphabet {" + word +"} -> count {" + count + "}");
			}
			
			@Override
			public void customDeclareOutputFields(AckingOutputFieldsDeclarer declarer) {
			}
		};
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SENTENCE_SPOUT, spout, 2).setNumTasks(20);
		
		builder.setBolt(SPLITER_BOLT, sentenceSplitBolt, 2)
				.shuffleGrouping(SENTENCE_SPOUT, SPOUT_SEND_STREAM)
				.setNumTasks(30);
		
		builder.setBolt(AGGREGATOR_BOLT, aggregatorBolt, 6)
				.shuffleGrouping(SPLITER_BOLT, SPLITTER_AGGREGATOR_SEND_STREAM)
				.setNumTasks(16);
		
		builder.setBolt(SUPERAGGREGATOR_BOLT, superAggregatorBolt, 6)
				.shuffleGrouping(AGGREGATOR_BOLT, AGGREGATOR_SUPERAGGREGATOR_SEND_STREAM)
				.setNumTasks(16);
		
		builder.setBolt(PRINTER_BOLT, printBolt, 3)
				.shuffleGrouping(SUPERAGGREGATOR_BOLT, SUPERAGGREGATOR_PRINT_SEND_STREAM)
				.setNumTasks(8);

		builder.addStreamTimeout(SPLITER_BOLT, AGGREGATOR_BOLT, SPLITTER_AGGREGATOR_SEND_STREAM, 10000L)
				.addStreamTimeout(AGGREGATOR_BOLT, SUPERAGGREGATOR_BOLT, AGGREGATOR_SUPERAGGREGATOR_SEND_STREAM, 20000L)
				.addStreamTimeout(SUPERAGGREGATOR_BOLT, PRINTER_BOLT, SUPERAGGREGATOR_PRINT_SEND_STREAM, 10000L);
		
		Config conf = new Config();
		conf.setDefaultPerEdgeTimeout(5000L);
		conf.setUseStormTimeoutMechanism(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(5);
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	
	}
		
}
