
package backtype.storm.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * Expires keys that have not been updated in the configured number of seconds.
 * The algorithm used will take between expirationSecs and
 * expirationSecs * (1 + 1 / (numBuckets-1)) to actually expire the message.
 *
 * get, put, remove, containsKey, and size take O(numBuckets) time to run.
 *
 * The advantage of this design is that the expiration thread only locks the object
 * for O(1) time, meaning the object is essentially always available for gets/puts.
 */
public class RotatingSet<K> {
    //this default ensures things expire at most 50% past the expiration time
    private static final int DEFAULT_NUM_BUCKETS = 3;

    public static interface ExpiredCallback<K> {
        public void expire(K key);
    }

    private LinkedList<HashSet<K>> _buckets;

    private ExpiredCallback _callback;
    
    public RotatingSet(int numBuckets, ExpiredCallback<K> callback) {
        if(numBuckets<2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _buckets = new LinkedList<HashSet<K>>();
        for(int i=0; i<numBuckets; i++) {
            _buckets.add(new HashSet<K>());
        }

        _callback = callback;
    }

    public RotatingSet(ExpiredCallback<K> callback) {
        this(DEFAULT_NUM_BUCKETS, callback);
    }

    public RotatingSet(int numBuckets) {
        this(numBuckets, null);
    }   
    
    public Set<K> rotate() {
        Set<K> dead = _buckets.removeLast();
        _buckets.addFirst(new HashSet<K>());
        if(_callback!=null) {
            for(K entry: dead) {
                _callback.expire(entry);
            }
        }
        return dead;
    }

    public boolean containsKey(K key) {
        for(HashSet<K> bucket: _buckets) {
            if(bucket.contains(key)) {
                return true;
            }
        }
        return false;
    }

    public void put(K key) {
        Iterator<HashSet<K>> it = _buckets.iterator();
        HashSet<K> bucket = it.next();
        bucket.add(key);
        while(it.hasNext()) {
            bucket = it.next();
            bucket.remove(key);
        }
    }
    
    
    public Object remove(K key) {
        for(HashSet<K> bucket: _buckets) {
            if(bucket.contains(key)) {
                return bucket.remove(key);
            }
        }
        return null;
    }

    public int size() {
        int size = 0;
        for(HashSet<K> bucket: _buckets) {
            size+=bucket.size();
        }
        return size;
    }    
}
