package storm.starter.faulttolerance;

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
public class SimpleWordCountTopology2 {

	private static final String SPOUT_SPLITTER_STREAM = "spoutSplitterStream";
	private static final String SPLITTER_EDGEAGGREGATOR_STREAM = "splitterEdgeAggregatorStream";
	private static final String EDGEAGGREGATOR_CENTRALAGGREGATOR_STREAM = "edgeAggregatorCentralaggregatorStream";
	private static final String CENTRALAGGREGATOR_PRINT_STREAM = "superAggregatorPrintStream";

	private static final String SPOUT = "sentencespout";
	private static final String SPLITER_BOLT = "splitterBolt";
	private static final String EDGEAGGREGATOR_BOLT = "edgeAggregatorBolt";
	private static final String CENTRALAGGREGATOR_BOLT = "centralAggregatorBolt";
	private static final String PRINTER_BOLT = "printerBolt";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int spoutParalellism = 2;
		int splitterParalellism = 2;
		int edgeParalellism = 8;
		int centreParalellism = 6;
		int printerParalellism = 3;
		
		boolean useStormTimeout = true;
		
		int numberOfWorkers = 3;
		int messageTimeout = 120;
		
		if(args.length > 1) {
			int argSize = args.length-1;
			spoutParalellism = Integer.parseInt(args[args.length-argSize]);
			argSize--;
			if(argSize > 0) {
				splitterParalellism = Integer.parseInt(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				edgeParalellism = Integer.parseInt(args[args.length-argSize]);
			}
			argSize--;
			if(argSize > 0) {
				centreParalellism = Integer.parseInt(args[args.length-argSize]);
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

		RegularRandomSentenceSpout spout = new RegularRandomSentenceSpout(SPOUT_SPLITTER_STREAM);
		RegularSplitterBolt splitterBolt = new RegularSplitterBolt(SPLITTER_EDGEAGGREGATOR_STREAM);
		RegularEdgeAggregatorBolt edAggregatorBolt = new RegularEdgeAggregatorBolt(EDGEAGGREGATOR_CENTRALAGGREGATOR_STREAM);
		RegularCentralAggregatorBolt centralAggregatorBolt = new RegularCentralAggregatorBolt(CENTRALAGGREGATOR_PRINT_STREAM);
		RegularPrintBolt printBolt = new RegularPrintBolt();
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SPOUT, spout, spoutParalellism);

		builder.setBolt(SPLITER_BOLT, splitterBolt, splitterParalellism).shuffleGrouping(SPOUT, SPOUT_SPLITTER_STREAM);

		builder.setBolt(EDGEAGGREGATOR_BOLT, edAggregatorBolt, edgeParalellism)
				.shuffleGrouping(SPLITER_BOLT, SPLITTER_EDGEAGGREGATOR_STREAM);

		builder.setBolt(CENTRALAGGREGATOR_BOLT, centralAggregatorBolt, centreParalellism)
				.shuffleGrouping(EDGEAGGREGATOR_BOLT, EDGEAGGREGATOR_CENTRALAGGREGATOR_STREAM);

		builder.setBolt(PRINTER_BOLT, printBolt, printerParalellism).shuffleGrouping(
				CENTRALAGGREGATOR_BOLT, CENTRALAGGREGATOR_PRINT_STREAM);

		Config conf = new Config();
		conf.setUseStormTimeoutMechanism(useStormTimeout);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(numberOfWorkers);
			conf.setMessageTimeoutSecs(messageTimeout);
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
						builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
