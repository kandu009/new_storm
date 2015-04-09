package storm.starter.faulttolerance;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;


public abstract class AbstractAckingBaseRichBolt extends AckingBaseRichBolt {

	private static final long serialVersionUID = 1L;

	public abstract void customExecute(Tuple tuple);
	
	@Override
	public void customPrepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		
	}

}