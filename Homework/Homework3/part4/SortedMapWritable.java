package part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

class SortedMapWritable extends AbstractMapWritable implements SortedMap<WritableComparable, Writable>{
	
	private SortedMap<WritableComparable, Writable> instance;
	
	private float rating;
	private long count;
	
	public SortedMapWritable() {
		super();
		this.instance = new TreeMap<WritableComparable, Writable>();
		this.rating = rating;
		this.count = count;
	}
	
	public SortedMapWritable(SortedMapWritable other) {
		this();
		copy(other);
	}

	public int size() {		
		return instance.size();
	}

	public boolean isEmpty() {
		return instance.isEmpty();
	}

	public boolean containsKey(Object key) {
		return instance.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return instance.containsValue(value);
	}

	public Writable get(Object key) {
		return instance.get(key);
	}

	public Writable put(WritableComparable key, Writable value) {
		addToMap(key.getClass());
		addToMap(value.getClass());
		return instance.put(key, value);
	}

	public Writable remove(Object key) {
		return instance.remove(key);
	}

	public void putAll(Map<? extends WritableComparable, ? extends Writable> m) {
		for (Map.Entry<? extends WritableComparable, ? extends Writable> e : m.entrySet()) {
			instance.put(e.getKey(), e.getValue());
		}
	} 

	public void clear() {
		instance.clear();
	}

	public Comparator<? super WritableComparable> comparator() {
		// TODO Auto-generated method stub
		return null;
	}

	public SortedMap<WritableComparable, Writable> subMap(WritableComparable fromKey, WritableComparable toKey) {
		return instance.subMap(fromKey, toKey);
	}

	public SortedMap<WritableComparable, Writable> headMap(WritableComparable toKey) {
		return instance.headMap(toKey);
	}

	public SortedMap<WritableComparable, Writable> tailMap(WritableComparable fromKey) {
		return instance.tailMap(fromKey);
	}

	public WritableComparable firstKey() {
		return instance.firstKey();
	}

	public WritableComparable lastKey() {
		return instance.lastKey();
	}

	public Set<WritableComparable> keySet() {
		return instance.keySet();
	}

	public Collection<Writable> values() {
		return instance.values();
	}

	public Set<Entry<WritableComparable, Writable>> entrySet() {
		return instance.entrySet();
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		
		int entries = in.readInt();
		
		for (int i = 0; i < entries; i++) {
			WritableComparable key =
					(WritableComparable) ReflectionUtils.newInstance(getClass(
							in.readByte()), getConf());
			
			key.readFields(in);
			
			Writable value = (Writable) ReflectionUtils.newInstance(getClass(
					in.readByte()), getConf());
			
			value.readFields(in);
			instance.put(key, value);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		
		out.writeInt(instance.size());
		
		for (Map.Entry<WritableComparable, Writable> e : instance.entrySet()) {
			out.writeByte(getId(e.getKey().getClass()));
			e.getKey().write(out);
			out.writeByte(getId(e.getValue().getClass()));
			e.getValue().write(out);
		}
	}

}
