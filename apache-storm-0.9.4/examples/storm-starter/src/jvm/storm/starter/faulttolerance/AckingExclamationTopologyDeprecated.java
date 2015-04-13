package storm.starter.faulttolerance;

import java.util.ArrayList;
import java.util.Arrays;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.task.TopologyContextConstants.Configuration;

@Deprecated
//this was initially written to get an idea of what all actions 
//needs to be performed for supporting per edge timeout mechanism
public class AckingExclamationTopologyDeprecated {

	public enum SendReceiveToken {
		send_msg,
		receive_msg,
		receive_ack;
	}
	
	private static String SPOUT = "word";
	private static String EXCLAIM_BOLT1 = "exclaim1";
	private static String EXCLAIM_BOLT2 = "exclaim2";
	private static String EXCLAIM_BOLT3 = "exclaim3";
	
	private static String SPOUT_SEND_STREAM = "SPOUT_SEND_STREAM";
	private static String B1_SEND_STREAM = "B1_SEND_STREAM";
	private static String B2_SEND_STREAM = "B2_SEND_STREAM";
	
	private static String B2_ACK_STREAM = "B2_ACK_STREAM";
	private static String B3_ACK_STREAM = "B3_ACK_STREAM";
	
	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SPOUT, new TestWordSpout(), 10)
				.addConfiguration(SendReceiveToken.send_msg.name(), SPOUT_SEND_STREAM);
		
		/*builder.setBolt(EXCLAIM_BOLT1, new AckingExclamationBoltDeprecated(), 3)
				.addConfiguration(SendReceiveToken.send_msg.name(), new ArrayList<SendStreamTimeoutPair>(Arrays.asList(new SendStreamTimeoutPair(B1_SEND_STREAM, 100))))
				.addConfiguration(SendReceiveToken.receive_msg.name(), new ArrayList<String>(Arrays.asList(SPOUT_SEND_STREAM)))
				.addConfiguration(Configuration.send_ack.name(), new ArrayList<String>()) // TODO: just added this to get some clarity on the initial design 
				.addConfiguration(SendReceiveToken.receive_ack.name(), new ArrayList<String>(Arrays.asList(B2_ACK_STREAM)))
				.shuffleGrouping(SPOUT)
				.shuffleGrouping(EXCLAIM_BOLT2, B2_ACK_STREAM);
		
		builder.setBolt(EXCLAIM_BOLT2, new AckingExclamationBoltDeprecated(), 2)
				.addConfiguration(SendReceiveToken.send_msg.name(), new ArrayList<SendStreamTimeoutPair>(Arrays.asList(new SendStreamTimeoutPair(B2_SEND_STREAM, 200))))
				.addConfiguration(SendReceiveToken.receive_msg.name(), new ArrayList<String>(Arrays.asList(B1_SEND_STREAM)))
				.addConfiguration(Configuration.send_ack.name(), new ArrayList<String>(Arrays.asList(B2_ACK_STREAM))) 
				.addConfiguration(SendReceiveToken.receive_ack.name(), new ArrayList<String>(Arrays.asList(B3_ACK_STREAM)))
				.shuffleGrouping(EXCLAIM_BOLT1, B1_SEND_STREAM)
				.shuffleGrouping(EXCLAIM_BOLT3, B3_ACK_STREAM);
		
		builder.setBolt(EXCLAIM_BOLT3, new AckingExclamationBoltDeprecated(), 2)
				.addConfiguration(SendReceiveToken.receive_msg.name(), new ArrayList<String>(Arrays.asList(B2_SEND_STREAM)))
				.addConfiguration(Configuration.send_ack.name(), new ArrayList<String>(Arrays.asList(B3_ACK_STREAM)))
				.shuffleGrouping(EXCLAIM_BOLT2, B2_SEND_STREAM);

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createTopology());
		}*/
	}

}
