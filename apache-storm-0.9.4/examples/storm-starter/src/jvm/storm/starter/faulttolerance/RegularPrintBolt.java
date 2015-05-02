package storm.starter.faulttolerance;

import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.AbstractAckingBaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.AckingOutputFieldsDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class RegularPrintBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	// this just gives you index in tuple which holds the incoming
	// message
	private static final int MESSAGE_INDEX_1 = 1;
	private static final int MESSAGE_INDEX_2 = 2;
	
	private static final Logger LOG = LoggerFactory.getLogger(RegularPrintBolt.class);
	
	private Random _rand;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_rand = new Random();
	}

	@Override
	public void execute(Tuple tuple) {
		
		// just like that
		Utils.sleep(Math.abs(_rand.nextInt() % 100));
		
		// As Spout is sending directly to this bolt and it provides no
		// other fancy stuff other than the message
		// which is a simple string in this case.
		String word = tuple.getString(MESSAGE_INDEX_1);
		Integer count = tuple.getInteger(MESSAGE_INDEX_2);
		LOG.info("Super Count for characer {" + word + "} -> count {" + count + "}");
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
}
