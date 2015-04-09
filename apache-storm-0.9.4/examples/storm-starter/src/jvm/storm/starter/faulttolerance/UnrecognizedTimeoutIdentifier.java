package storm.starter.faulttolerance;

public class UnrecognizedTimeoutIdentifier extends Exception {

	private static final long serialVersionUID = 1L;
	
	private String msg_;
	
	public UnrecognizedTimeoutIdentifier(String msg) {
		msg_ = msg;
	}
	
	public String getLocalizedMessage() {
		return msg_;
	}
	
}
