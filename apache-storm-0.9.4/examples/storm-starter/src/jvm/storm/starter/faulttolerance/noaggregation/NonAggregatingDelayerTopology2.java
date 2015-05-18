package storm.starter.faulttolerance.noaggregation;

import storm.starter.faulttolerance.AckingWordCountTopology2;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * 
 * @author rkandur
 *
 *         another use case of Per Edge Acking topology. using this to conduct
 *         experiments This is more easier to realize as a topology which can
 *         have intentional delays.
 * 
 *         difference between {@link AckingWordCountTopology2} and this is here
 *         we are just using storm's per topology timeout and
 *         {@link AckingWordCountTopology2} is using per edge timeout
 * 
 */
public class NonAggregatingDelayerTopology2 {

	private static final String SPOUT_TRANSFORM_STREAM = "sts";
	private static final String TRANSFORM_DELAYER_STREAM = "tds";
	private static final String DELAYER_PRINT_STREAM = "dps";

	private static final String SPOUT = "s";
	private static final String TRANSFORM_BOLT = "tb";
	private static final String DELAYER_BOLT = "db";
	private static final String PRINTER_BOLT = "pb";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int spoutParalellism = 2;
		int transformerParalellism = 2;
		int delayerParalellism = 8;
		int printerParalellism = 3;
		
		boolean useStormTimeout = true;
		
		int numberOfWorkers = 3;
		int messageTimeout = 120;
		
		if(args.length > 1) {
			int argSize = args.length-1;
			spoutParalellism = Integer.parseInt(args[args.length-argSize]);
			argSize--;
			if(argSize > 0) {
				transformerParalellism = Integer.parseInt(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				delayerParalellism = Integer.parseInt(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				printerParalellism = Integer.parseInt(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				useStormTimeout = args[args.length-argSize].toLowerCase().equals("true") ? true : false;
			}
			argSize--;
			if(argSize > 0) {
				numberOfWorkers = Integer.parseInt(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				messageTimeout = Integer.parseInt(args[args.length-argSize]);
			}
			
		}

		NonAggregatingRandomWordSpout spout = new NonAggregatingRandomWordSpout(SPOUT_TRANSFORM_STREAM);
		NonAggregatingTransformBolt transformBolt = new NonAggregatingTransformBolt(TRANSFORM_DELAYER_STREAM);
		NonAggregatingDelayerBolt delayerBolt = new NonAggregatingDelayerBolt(DELAYER_PRINT_STREAM);
		NonAggregatingPrintBolt printBolt = new NonAggregatingPrintBolt();
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SPOUT, spout, spoutParalellism);

		builder.setBolt(TRANSFORM_BOLT, transformBolt, transformerParalellism)
				.shuffleGrouping(SPOUT, SPOUT_TRANSFORM_STREAM);

		builder.setBolt(DELAYER_BOLT, delayerBolt, delayerParalellism)
				.shuffleGrouping(TRANSFORM_BOLT, TRANSFORM_DELAYER_STREAM);

		builder.setBolt(PRINTER_BOLT, printBolt, printerParalellism).shuffleGrouping(
				DELAYER_BOLT, DELAYER_PRINT_STREAM);

		Config conf = new Config();
		conf.setUseStormTimeoutMechanism(useStormTimeout);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(numberOfWorkers);
			if(useStormTimeout) { conf.setMessageTimeoutSecs(messageTimeout); }
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
						builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
