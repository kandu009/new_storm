package storm.starter.aggregation;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class FailingBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

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
	}
	
	public void execute(Tuple tuple) {
		_collector.emit(tuple, new Values(tuple.getString(0) + tuple.getString(0)));
		updateAckOrFailCount(EMITTED);
		_collector.ack(tuple);
		updateAckOrFailCount(ACKED);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
}
