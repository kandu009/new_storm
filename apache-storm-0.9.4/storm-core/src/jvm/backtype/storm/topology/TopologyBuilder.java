/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.topology;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.security.acl.Group;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONValue;

import backtype.storm.task.AckingBaseRichBolt;
import backtype.storm.task.TopologyContextConstants.Configuration;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Storm
 * to execute. Topologies are Thrift structures in the end, but since the Thrift API
 * is so verbose, TopologyBuilder greatly eases the process of creating topologies.
 * The template for creating and submitting a topology looks something like:
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * 
 * StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
 * </pre>
 *
 * Running the exact same topology in local mode (in process), and configuring it to log all tuples
 * emitted, looks like the following. Note that it lets the topology run for 10 seconds
 * before shutting down the local cluster.
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * conf.put(Config.TOPOLOGY_DEBUG, true);
 *
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology("mytopology", conf, builder.createTopology());
 * Utils.sleep(10000);
 * cluster.shutdown();
 * </pre>
 *
 * <p>The pattern for TopologyBuilder is to map component ids to components using the setSpout
 * and setBolt methods. Those methods return objects that are then used to declare
 * the inputs for that component.</p>
 */
public class TopologyBuilder {
    private Map<String, IRichBolt> _bolts = new HashMap<String, IRichBolt>();
    private Map<String, IRichSpout> _spouts = new HashMap<String, IRichSpout>();
    private Map<String, ComponentCommon> _commons = new HashMap<String, ComponentCommon>();

//    private Map<String, Map<GlobalStreamId, Grouping>> _inputs = new HashMap<String, Map<GlobalStreamId, Grouping>>();

    private Map<String, StateSpoutSpec> _stateSpouts = new HashMap<String, StateSpoutSpec>();
    
    private static String TIMEOUT_SEPARATOR = "#";
    private static String ACKING_STREAM_ID_SEPARATOR = "_";
    private static String TIMEOUT_ID_SEPARATOR = "_";
    private static String ACKING_STREAM_DELIMITER = "|";
    private static String TIMEOUT_ID_DELIMITER = "|";
    
    private boolean isAckingComponent(GlobalStreamId gsi) {
    	String cid = gsi.get_componentId();
    	if(_spouts.containsKey(cid)) {
    		// for now we are only worried about per edge Acking bolts
    		// take care of this, when we consider acking spouts
    		return false;
    	}  else if (_bolts.containsKey(cid)) {
    		return (_bolts.get(cid) instanceof AckingBaseRichBolt) ? true : false;
    	}
    	return false;
    }
    
    public StormTopology createTopology() {
    	
    	// <componentId, StreamID> this should be added to the components inputs using _commons
    	HashMap<String, HashMap<GlobalStreamId, Grouping>> addToInputs = 
    			new HashMap<String, HashMap<GlobalStreamId,Grouping>>();
    	
    	// <componentId, ackingStreamId's>
    	HashMap<String, String> compIdVsAckingStreams = new HashMap<String, String>();
    	
    	for(String boltId: _bolts.keySet()) {
            
            if(_bolts.get(boltId) instanceof AckingBaseRichBolt) {
            	
            	// <globalStreamID = (streamID,componentID), grouping>>
            	Map<GlobalStreamId, Grouping> sources = getSources(boltId);

            	StringBuilder ackingStreams = new StringBuilder();

            	for(GlobalStreamId srcGlobStrId: sources.keySet()) {

            		// TODO: should we be sending acks to sources which are not
    				// participating in per edge timeout mechanism at all? will
    				// sending acks to others be used ? I don't think so. !!!
            		if(!isAckingComponent(srcGlobStrId)) {
            			continue;
            		}
            		
            		// this means that the ack stream is named after the targetComponentId, sourceComponentId and streamId
            		// even if re-balance / a new node takes this component, as the component Id remains the same,
            		// we will not see any issues with loose ends in the streamId's added for acknowledgement's
            		String ackingStreamId = boltId 
							+ ACKING_STREAM_ID_SEPARATOR 
							+ srcGlobStrId.get_componentId()
							+ ACKING_STREAM_ID_SEPARATOR
							+ srcGlobStrId.get_streamId();
            		
            		// adding the ack stream to the inputs of the source of AckingBolt
            		HashMap<GlobalStreamId, Grouping> old = new HashMap<GlobalStreamId, Grouping>();
            		if(addToInputs.containsKey(srcGlobStrId.get_componentId())) {
            			old  = addToInputs.get(srcGlobStrId.get_componentId());
            		}
            		GlobalStreamId gsid = new GlobalStreamId(boltId, ackingStreamId);
            		old.put(gsid, Grouping.shuffle(new NullStruct()));
            		addToInputs.put(srcGlobStrId.get_componentId(), old);
            		_commons.get(boltId).put_to_streams(ackingStreamId, new StreamInfo(new ArrayList<String>(), false));
            		
            		ackingStreams.append(ackingStreamId).append(ACKING_STREAM_DELIMITER);
            		
            	}
            	if(-1 != ackingStreams.lastIndexOf(ACKING_STREAM_DELIMITER)) {
            		ackingStreams.deleteCharAt(ackingStreams.lastIndexOf(ACKING_STREAM_DELIMITER));
            	}
            	
            	// we should be adding this stream to the current bolts configuration to make 
                // sure that we are sending the ack message on the ackingStreamId correctly
            	compIdVsAckingStreams.put(boltId, ackingStreams.toString());
            	
            }
            
        }
    	
        Map<String, Bolt> boltSpecs = new HashMap<String, Bolt>();
        Map<String, SpoutSpec> spoutSpecs = new HashMap<String, SpoutSpec>();
        
        for(String boltId: _bolts.keySet()) {
        
        	IRichBolt bolt = _bolts.get(boltId);
            
        	// adding the ack stream to the inputs of the source of AckingBolt
            if(addToInputs.containsKey(boltId)) {
            	HashMap<GlobalStreamId, Grouping> newInputs = addToInputs.get(boltId);
            	for(GlobalStreamId ni : newInputs.keySet()) {
	            	_commons.get(boltId).put_to_inputs(ni, newInputs.get(ni));
            	}
            }
            
            // we should be adding acking stream to the respective bolt configuration to make 
            // sure that we are sending the ack message on this ackingStreamId correctly
            if(compIdVsAckingStreams.containsKey(boltId)) {
            	Map currConfMap = parseJson(_commons.get(boltId).get_json_conf());
        		if(currConfMap.containsKey(Configuration.send_ack.name())) {
        			Object oldValue = currConfMap.get(Configuration.send_ack.name());
        			// RKNOTE: using '|' as delimiter for separating multiple stream names
        			currConfMap.put(Configuration.send_ack.name(), 
        					oldValue.toString()+ACKING_STREAM_DELIMITER+compIdVsAckingStreams.get(boltId));
        		} else {
        			currConfMap.put(Configuration.send_ack.name(), compIdVsAckingStreams.get(boltId));
        		}
        		_commons.get(boltId).set_json_conf(JSONValue.toJSONString(currConfMap));
            }
            
            ComponentCommon common = getComponentCommon(boltId, bolt);
            boltSpecs.put(boltId, new Bolt(ComponentObject.serialized_java(Utils.serialize(bolt)), common));
        }
        
        for(String spoutId: _spouts.keySet()) {
            IRichSpout spout = _spouts.get(spoutId);
            ComponentCommon common = getComponentCommon(spoutId, spout);
            spoutSpecs.put(spoutId, new SpoutSpec(ComponentObject.serialized_java(Utils.serialize(spout)), common));
        
        }
        
        return new StormTopology(spoutSpecs,
                                 boltSpecs,
                                 new HashMap<String, StateSpoutSpec>());
    }

	//TODO: RK added
    public Set<String> getComponentIds() {
        Set<String> ret = new HashSet<String>();
        ret.addAll(_spouts.keySet());
        ret.addAll(_bolts.keySet());
        return ret;
    }

    //TODO: RK added
	/**
	 * Gets information about who is consuming the outputs of the specified
	 * component, and how.
	 * 
	 * @return Map from stream id to component id to the Grouping used.
	 */
    public Map<String, Map<String, Grouping>> getTargets(String componentId) {

    	Map<String, Map<String, Grouping>> ret = new HashMap<String, Map<String, Grouping>>();
        for(String otherComponentId: getComponentIds()) {
        	
        	ComponentCommon compCommon = getComponentCommon(otherComponentId);
        	if(compCommon == null) {
        		continue;
        	}
            Map<GlobalStreamId, Grouping> inputs = compCommon.get_inputs();
            for(GlobalStreamId id: inputs.keySet()) {
                if(id.get_componentId().equals(componentId)) {
                    Map<String, Grouping> curr = ret.get(id.get_streamId());
                    if(curr==null) {
                    	curr = new HashMap<String, Grouping>();
                    }
                    curr.put(otherComponentId, inputs.get(id));
                    ret.put(id.get_streamId(), curr);
                }
            }
        }
        return ret;
    }
    
    //TODO: RK added
	/**
	 * Gets the declared inputs to the specified component.
	 * 
	 * @return A map from subscribed component/stream to the grouping subscribed with.
	 */
    public Map<GlobalStreamId, Grouping> getSources(String componentId) {
    	return getComponentCommon(componentId).get_inputs();
    }
    
    //TODO: RK added
	/**
	 * Gets the {@link ComponentCommon} object of the Component with this 
	 * @param componentId
	 * 
	 * @return {@link ComponentCommon}
	 */
    public ComponentCommon getComponentCommon(String componentId) {
    	 return _commons.get(componentId);
	}
    
    /**
     * Define a new bolt in this topology with parallelism of just one thread.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt) {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
        validateUnusedId(id);
        initCommon(id, bolt, parallelism_hint);
        _bolts.put(id, bolt);
        return new BoltGetter(id);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint);
    }

    /**
     * Define a new spout in this topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout) {
        return setSpout(id, spout, null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism. If the spout declares
     * itself as non-distributed, the parallelism_hint will be ignored and only one task
     * will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
     * @param spout the spout
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) {
        validateUnusedId(id);
        initCommon(id, spout, parallelism_hint);
        _spouts.put(id, spout);
        return new SpoutGetter(id);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout) {
        setStateSpout(id, stateSpout, null);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout, Number parallelism_hint) {
        validateUnusedId(id);
        // TODO: finish
    }


    private void validateUnusedId(String id) {
        if(_bolts.containsKey(id)) {
            throw new IllegalArgumentException("Bolt has already been declared for id " + id);
        }
        if(_spouts.containsKey(id)) {
            throw new IllegalArgumentException("Spout has already been declared for id " + id);
        }
        if(_stateSpouts.containsKey(id)) {
            throw new IllegalArgumentException("State spout has already been declared for id " + id);
        }
    }

    private ComponentCommon getComponentCommon(String id, IComponent component) {

    	ComponentCommon ret = new ComponentCommon(_commons.get(id));
        
    	Map<String,StreamInfo> streams = new HashMap<String, StreamInfo>();
        if(ret.get_streams() != null && !ret.get_streams().isEmpty()) {
        	streams = ret.get_streams();
        }
        
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        streams.putAll(getter.getFieldsDeclaration());
        ret.set_streams(streams);
        return ret;        
    }

	private void initCommon(String id, IComponent component, Number parallelism) {
        ComponentCommon common = new ComponentCommon();
        common.set_inputs(new HashMap<GlobalStreamId, Grouping>());
        if(parallelism!=null) common.set_parallelism_hint(parallelism.intValue());
        Map conf = component.getComponentConfiguration();
        if(conf!=null) common.set_json_conf(JSONValue.toJSONString(conf));
        _commons.put(id, common);
    }

    protected class ConfigGetter<T extends ComponentConfigurationDeclarer> extends BaseConfigurationDeclarer<T> {
        String _id;
        
        public ConfigGetter(String id) {
            _id = id;
        }
        
        @Override
        public T addConfigurations(Map conf) {
            if(conf!=null && conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
                throw new IllegalArgumentException("Cannot set serializations for a component using fluent API");
            }
            String currConf = _commons.get(_id).get_json_conf();
            _commons.get(_id).set_json_conf(mergeIntoJson(parseJson(currConf), conf));
            return (T) this;
        }
    }
    
    protected class SpoutGetter extends ConfigGetter<SpoutDeclarer> implements SpoutDeclarer {
        public SpoutGetter(String id) {
            super(id);
        }        
    }
    
    protected class BoltGetter extends ConfigGetter<BoltDeclarer> implements BoltDeclarer {
        private String _boltId;

        public BoltGetter(String boltId) {
            super(boltId);
            _boltId = boltId;
        }

        // TODO: RK added
        public BoltDeclarer streamTimeout(String sourceId, String targetId, String streamId, Long timeout) {
            return addTimeout(sourceId, targetId, streamId, timeout);
        }

        private BoltDeclarer addTimeout(String sourceId, String targetId, String streamId, Long timeout) {
        	
        	//we add sourceId+"_"+targetId+"_"+streamId -> timeout value i the configuration of both source and target
        	// at source, we need this to track tuples using RotatingMap
        	// TODO: at target, do we need this at all ? 
        	Map currSourceConf = parseJson(_commons.get(sourceId).get_json_conf());
        	String key = sourceId+TIMEOUT_ID_SEPARATOR+targetId+TIMEOUT_ID_SEPARATOR+streamId;
        	String sValue = new String().concat(key).concat(TIMEOUT_SEPARATOR).concat(timeout.toString());

        	if(currSourceConf.containsKey(Configuration.timeout.name())) {
        		sValue.concat(TIMEOUT_ID_DELIMITER).concat((String) currSourceConf.get(Configuration.timeout.name()));
        	}
        	Map sConf = new HashMap();
            sConf.put(key, sValue);
        	_commons.get(sourceId).set_json_conf(mergeIntoJson(currSourceConf, sConf));

        	Map currTargetConf = parseJson(_commons.get(targetId).get_json_conf());
        	String tValue = new String().concat(key).concat(TIMEOUT_SEPARATOR).concat(timeout.toString());
        	if(currTargetConf.containsKey(Configuration.timeout.name())) {
        		tValue.concat(TIMEOUT_ID_DELIMITER).concat((String) currTargetConf.get(Configuration.timeout.name()));
        	}
        	Map tConf = new HashMap();
            tConf.put(key, tValue);
        	_commons.get(targetId).set_json_conf(mergeIntoJson(currTargetConf, tConf));
        	
			return this;
		}
        
        public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
            return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
        }

        public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
            return grouping(componentId, streamId, Grouping.fields(fields.toList()));
        }

        public BoltDeclarer globalGrouping(String componentId) {
            return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer globalGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
        }

        public BoltDeclarer shuffleGrouping(String componentId) {
            return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.shuffle(new NullStruct()));
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId) {
            return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.local_or_shuffle(new NullStruct()));
        }
        
        public BoltDeclarer noneGrouping(String componentId) {
            return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer noneGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.none(new NullStruct()));
        }

        public BoltDeclarer allGrouping(String componentId) {
            return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer allGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.all(new NullStruct()));
        }

        public BoltDeclarer directGrouping(String componentId) {
            return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer directGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.direct(new NullStruct()));
        }

        private BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, streamId), grouping);
            return this;
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
            return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
            return grouping(componentId, streamId, Grouping.custom_serialized(Utils.serialize(grouping)));
        }

        @Override
        public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
            return grouping(id.get_componentId(), id.get_streamId(), grouping);
        }        
    }
    
    private static Map parseJson(String json) {
        if(json==null) return new HashMap();
        else return (Map) JSONValue.parse(json);
    }
    
    private static String mergeIntoJson(Map into, Map newMap) {
        Map res = new HashMap(into);
        if(newMap!=null) res.putAll(newMap);
        return JSONValue.toJSONString(res);
    }
}