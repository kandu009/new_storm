package storm.starter.faulttolerance.delayer.acking;

import storm.starter.faulttolerance.SimpleAckingWordCountTopology2;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * 
 * @author rkandur
 *
 *         another use case of Per Edge Acking topology. using this to conduct
 *         experiments This just adds intentional delays, but there is no
 *         aggregation of data that is being done.
 * 
 *         This is basically to see if the acknowledgement's are getting delayed
 *         by a polynomial order of time which is the case with
 *         aggregation+delayed topology like
 *         {@link SimpleAckingWordCountTopology2}
 * 
 */
public class AckingDelayerTopology2 {

	private static final String SPOUT_TRANSFORM_STREAM = "sts";
	private static final String TRANSFORM_DELAYER_STREAM = "tds";
	private static final String DELAYER_PRINT_STREAM = "dps";

	private static final String SPOUT = "s";
	private static final String TRANSFORMER_BOLT = "tb";
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

		long transformDelayTimeout = 300000L;
		long delayPrintTimeout = 250000L;
		long defaultPerEdgeTimeout = 100L;
		
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
				transformDelayTimeout = Long.parseLong(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				delayPrintTimeout = Long.parseLong(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				defaultPerEdgeTimeout = Long.parseLong(args[args.length-argSize]);
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

		AckingDelayerRandomWordSpout spout = new AckingDelayerRandomWordSpout(SPOUT_TRANSFORM_STREAM);
		AckingDelayerTransformBolt transformBolt = new AckingDelayerTransformBolt(TRANSFORM_DELAYER_STREAM);
		AckingDelayerBolt delayerBolt = new AckingDelayerBolt(DELAYER_PRINT_STREAM);
		AckingDelayerPrintBolt printBolt = new AckingDelayerPrintBolt();
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SPOUT, spout, spoutParalellism);

		builder.setBolt(TRANSFORMER_BOLT, transformBolt, transformerParalellism)
				.shuffleGrouping(SPOUT, SPOUT_TRANSFORM_STREAM);

		builder.setBolt(DELAYER_BOLT, delayerBolt, delayerParalellism)
				.shuffleGrouping(TRANSFORMER_BOLT, TRANSFORM_DELAYER_STREAM);

		builder.setBolt(PRINTER_BOLT, printBolt, printerParalellism).shuffleGrouping(
				DELAYER_BOLT, DELAYER_PRINT_STREAM);

		builder.addStreamTimeout(TRANSFORMER_BOLT, DELAYER_BOLT, TRANSFORM_DELAYER_STREAM, transformDelayTimeout)
				.addStreamTimeout(DELAYER_BOLT, PRINTER_BOLT, DELAYER_PRINT_STREAM, delayPrintTimeout);

		Config conf = new Config();
		conf.setDefaultPerEdgeTimeout(defaultPerEdgeTimeout);
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
