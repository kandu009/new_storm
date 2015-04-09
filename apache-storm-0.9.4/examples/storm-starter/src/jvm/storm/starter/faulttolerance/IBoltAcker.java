package storm.starter.faulttolerance;

import backtype.storm.task.IBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author rkandur
 * 
 * An interface which defines a policy for all the bolt acker modules that go
 * into the individual {@link IBolt} implementations as per
 * the topology/requirement.
 * 
 * TODO : do we even need this? shouldn't this go into 
 * the implementation part of the IAckingBolt ack() definition ?
 */
public interface IBoltAcker {

	/**
	 * method which indicates the acknowledgement for the tuple emitted with an id = @param id
	 */
	void update_ack(Tuple tuple);
	
}
