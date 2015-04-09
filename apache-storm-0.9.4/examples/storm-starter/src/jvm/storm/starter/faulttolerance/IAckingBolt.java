package storm.starter.faulttolerance;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * @author rkandur
 * 
 * An interface which defines a policy for all the bolts 
 * which should suppport a local acking mechanism on top of the 
 * existing acking mechanism provided by storm.
 * 
 */
public interface IAckingBolt extends IBoltAcker {

	 /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the bolt with the environment in which the bolt executes.
     *
     * <p>This includes the:</p>
     * 
     * @param stormConf The Storm configuration for this bolt. This is the configuration 
     * 			provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, 
     * 			including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, 
     * 		including the prepare and cleanup methods. 
     * 
     * TODO: The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    void prepare(Map stormConf, TopologyContext context, IAckingOutputCollector collector);

    /**
     * Process a single tuple of input. The Tuple object contains metadata on it
     * about which component/stream/task it came from. The values of the Tuple can
     * be accessed using Tuple#getValue. 
     * 
     * <p>Tuples should be emitted using the IAckingOutputCollector provided through the prepare method.
     * It is required that all input tuples are acked or failed at some 
     * point using the IAckingOutputCollector if we wish to enable acking at a bolt/storm level.
     *
     * @param input The input tuple to be processed.
     */
    void execute(Tuple input);

    /**
     * Called when an {@link IAckingBolt} is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     *
     * <p>The one context where cleanup is guaranteed to be called is when a topology
     * is killed when running Storm in local mode.</p>
     */
    void cleanup();
	
}
