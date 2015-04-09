package storm.starter.aggregation;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AggregatorBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	public enum Categories {
		
		apple("apple", 1000),
		banana("banana", 2000),
		papaya("papaya", 3000),
		grapes("grapes", 4000),
		cranberry("cranberry", 5000),
		invalid("invalid", -1);
		
		String _value;
		Integer _threshold;
		
		Categories(String value, Integer threshold) {
			_value = value;
			_threshold = threshold;
		}
		
		public String getName() {
			return _value;
		}
		
		public Integer getThreshold() {
			return _threshold;
		}
		
		public Categories getCategoryFor(String v) {
			for(Categories cs : Categories.values()) {
				if(cs.getName().equals(v)) {
					return cs;
				}
			}
			
			return Categories.invalid;
		}
	}
	
	private static long TIME_WINDOW = 60000;
	private static long STARTUP_TIME = 120000;
	
	OutputCollector _collector;
	
	private static Timer _timer;
	HashMap<String, Integer> _tupleCounterMap;
	HashMap<String, Integer> _tupleCounterThresholdMap;
	
	private void initializeMaxCountStore() {
		_tupleCounterThresholdMap = new HashMap<String, Integer>();
		for(Categories c: Categories.values()) {
			if(!c.equals(Categories.invalid)) {
				_tupleCounterThresholdMap.put(c.getName(), c.getThreshold());
			}
		}
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_tupleCounterMap = new HashMap<String, Integer>();
		initializeMaxCountStore();
	}
	
	public void execute(Tuple tuple) {
		
		if(_timer == null) {
			_timer = new Timer("DelayerTimer", true);
			TimerTask task = new TimerTask() {
				public void run() {
					if(!_tupleCounterMap.isEmpty()) {
						for(String s : _tupleCounterMap.keySet()) {
							if(0 != _tupleCounterMap.get(s)) {
								_collector.emit(tuple, new Values(s));
							    _collector.ack(tuple);
							}
						}
					}
				}
			};
			_timer.scheduleAtFixedRate(task, STARTUP_TIME, TIME_WINDOW);
		}
		
		if(!_tupleCounterMap.isEmpty()) {
			for(String s : _tupleCounterMap.keySet()) {
				if(_tupleCounterMap.get(s).equals(_tupleCounterThresholdMap.get(s))) {
					_collector.emit(tuple, new Values(s));
				    _collector.ack(tuple);
				}
			}
		}
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	

}
