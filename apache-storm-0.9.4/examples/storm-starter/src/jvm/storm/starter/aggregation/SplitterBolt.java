package storm.starter.aggregation;

import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class SplitterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	ArrayList<Character> _appenders = new ArrayList<Character>();
	
	OutputCollector _collector;
	
	private static String EMITTED = "emitted";
	private static String ACKED = "acked";

	private long _acked = 0;
	private long _emitted = 0;
	
	private void updateAckOrFailCount(String key) {
       if(key.toLowerCase().equals(ACKED)) {
    	   _acked++;
       } else if(key.toLowerCase().equals(EMITTED)) {
    	   _emitted++;
       } 
    }
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		initializeAppenderList();
	}
	
	private void initializeAppenderList () {
		_appenders.add('A');
		_appenders.add('B');
		_appenders.add('C');
		_appenders.add('D');
		_appenders.add('E');
	}
	
	public void execute(Tuple tuple) {
		// randomly generating some new tuples based on one source tuple
		for(Character c : _appenders) {
			_collector.emit(tuple, new Values(tuple.getString(0) + c.toString()));
		}
		updateAckOrFailCount(EMITTED);
		
		_collector.ack(tuple);
		updateAckOrFailCount(ACKED);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
}
