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
 *         difference between {@link AckingWordCountTopology1} and this is here
 *         we are adding intentional delays while data is transmitted kind of
 *         simulating the delays due to windowed aggregation sort of
 *         applications
 * 
 */
public class AckingWordCountTopology2 {

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

		AckingRandomSentenceSpout spout = new AckingRandomSentenceSpout(SPOUT_SPLITTER_STREAM);
		AckingSplitterBolt splitterBolt = new AckingSplitterBolt(SPLITTER_EDGEAGGREGATOR_STREAM);
		AckingEdgeAggregatorBolt edAggregatorBolt = new AckingEdgeAggregatorBolt(EDGEAGGREGATOR_CENTRALAGGREGATOR_STREAM);
		AckingCentralAggregatorBolt centralAggregatorBolt = new AckingCentralAggregatorBolt(CENTRALAGGREGATOR_PRINT_STREAM);
		AckingPrintBolt printBolt = new AckingPrintBolt();
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SPOUT, spout, 2);

		builder.setBolt(SPLITER_BOLT, splitterBolt, 2).shuffleGrouping(SPOUT,
				SPOUT_SPLITTER_STREAM);

		builder.setBolt(EDGEAGGREGATOR_BOLT, edAggregatorBolt, 8)
				.shuffleGrouping(SPLITER_BOLT, SPLITTER_EDGEAGGREGATOR_STREAM);

		builder.setBolt(CENTRALAGGREGATOR_BOLT, centralAggregatorBolt, 6)
				.shuffleGrouping(EDGEAGGREGATOR_BOLT, EDGEAGGREGATOR_CENTRALAGGREGATOR_STREAM);

		builder.setBolt(PRINTER_BOLT, printBolt, 3).shuffleGrouping(
				CENTRALAGGREGATOR_BOLT, CENTRALAGGREGATOR_PRINT_STREAM);

		builder.addStreamTimeout(SPLITER_BOLT, EDGEAGGREGATOR_BOLT, SPLITTER_EDGEAGGREGATOR_STREAM, 100000L)	// 50
			.addStreamTimeout(EDGEAGGREGATOR_BOLT, CENTRALAGGREGATOR_BOLT, EDGEAGGREGATOR_CENTRALAGGREGATOR_STREAM, 200000L)	//2500
			.addStreamTimeout(CENTRALAGGREGATOR_BOLT, PRINTER_BOLT, CENTRALAGGREGATOR_PRINT_STREAM, 100000L);	//4200

		Config conf = new Config();
		conf.setDefaultPerEdgeTimeout(100L);
		conf.setUseStormTimeoutMechanism(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(4);
			conf.setMessageTimeoutSecs(60);
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
						builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
