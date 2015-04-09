package storm.starter.faulttolerance;

import java.util.Collection;
import java.util.List;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * @author rkandur
 * 
 * An interface which defines a policy for a Collector which will be used for 
 * enabling internal acking mechanism on top of an aching mechanism which storm 
 * provides.
 * 
 * This is one of the kind {@link IOutputCollector} or {@link ISpoutOutputCollector}
 * 
 */
public interface IAckingOutputCollector {

	/**
	 * Emits the tuple and returns the task ID's of the components to which the tuple has been emitted
	 * 
	 * @param streamId	: streamID on which the tuple is emitted
	 * @param anchors	: The {@link Tuple} set which serves as an achor to the new {@link Tuple} being emitted
	 * @param tuple		: The new {@link Tuple} that is being emitted
	 * @return
	 */
	 List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple);
	 
	 void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple);
	 
	 void ack(Tuple input);
	 
	 void fail(Tuple input);
	
}
