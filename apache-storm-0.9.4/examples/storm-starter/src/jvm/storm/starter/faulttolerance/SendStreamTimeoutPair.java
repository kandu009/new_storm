package storm.starter.faulttolerance;

public class SendStreamTimeoutPair {

	private String streamId_;
	private long timeout_;
	
	public SendStreamTimeoutPair(String id, Integer timeout) {
		setStreamId(id);
		setTimeout(timeout); 
	}

	public String getStreamId() {
		return streamId_;
	}

	public void setStreamId(String streamId_) {
		this.streamId_ = streamId_;
	}

	public long getTimeout() {
		return timeout_;
	}

	public void setTimeout(long timeout_) {
		this.timeout_ = timeout_;
	}
	
}
