package storm.starter.aggregation;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class GeneratorSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private static String FAILED = "failed";
	private static String ACKED = "acked";
	
	SpoutOutputCollector _collector;
	
	private long _acked = 0;
	private long _failed = 0;
	
	private void updateAckOrFailCount(String key) {
       if(key.toLowerCase().equals(ACKED)) {
    	   _acked++;
       } else if(key.toLowerCase().equals(FAILED)) {
    	   _failed++;
       } 
    }
	
	public GeneratorSpout() {

	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void nextTuple() {
//		Utils.sleep(100);
		final String[] wordsToEmit = new String[] { 
											"apple", 
											"banana", 
											"papaya",
											"grapes", 
											"cranberry" 
										  };
		final Random rand = new Random();
		final String wordToEmit = wordsToEmit[rand.nextInt(wordsToEmit.length)];
		String msgId = new String().concat(wordToEmit).concat(new StringBuilder().append(rand.nextInt()).toString());
		_collector.emit(new Values(wordToEmit), msgId);
	}

	public void ack(Object msgId) {
		updateAckOrFailCount(ACKED);
	}

	public void fail(Object msgId) {
		updateAckOrFailCount(FAILED);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return new HashMap<String, Object>();
	}
	
	public long getFailedTupleCount() {
		return _failed;
	}
	
	public long getAckedTupleCount() {
		return _acked;
	}
}
