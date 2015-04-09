package backtype.storm.task;

public class TopologyContextConstants {

	public static enum Configuration {
		timeout,
		send_ack;
	}
	
	public static Long DEFAULT_PER_EDGE_TIMEOUT = 1000L;
	
}
