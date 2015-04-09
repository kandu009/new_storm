package storm.starter.aggregation;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TimeoutTopology {

	public static void main(String[] args) {

		int SPOUT_PARALLELISM = 5;
		int SPLITTER_PARALLELISM = 5;
		int FORWARDER_PARALLELISM = 5;
		int AGGREGATOR_PARALLELISM = 1;

		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("generator", new GeneratorSpout(), SPOUT_PARALLELISM);
		builder.setBolt("splitter", new SplitterBolt(), SPLITTER_PARALLELISM)
		       .shuffleGrouping("generator");
		builder.setBolt("forwarder", new ForwardingBolt(), FORWARDER_PARALLELISM)
	           .shuffleGrouping("splitter");
		builder.setBolt("aggregator", new AggregatorBolt(), AGGREGATOR_PARALLELISM)
	       .shuffleGrouping("forwarder");
		
		Config conf = new Config();
	    conf.setDebug(true);
	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);
	      try {
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	    } else {
	    	System.out.println("Invalid arguments while submitting storm topology !");
	    }
	}

}