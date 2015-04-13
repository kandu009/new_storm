package backtype.storm.topology;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.generated.StreamInfo;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class AckingOutputFieldsDeclarer {

	 private Map<String, StreamInfo> _fields = new HashMap<String, StreamInfo>();

	    public void declare(Fields fields) {
	        declare(false, fields);
	    }

	    public void declare(boolean direct, Fields fields) {
	        declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
	    }

	    public void declareStream(String streamId, Fields fields) {
	        declareStream(streamId, false, fields);
	    }

	    public void declareStream(String streamId, boolean direct, Fields fields) {
	        _fields.put(streamId, new StreamInfo(fields.toList(), direct));
	    }

	    public Map<String, StreamInfo> getFieldsDeclaration() {
	        return _fields;
	    }
	
}
