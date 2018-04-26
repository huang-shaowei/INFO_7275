package nyse_wo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable>  {

	
	private String symbol;
	private Long max_vol;
	private String maxvol_date;
	private Long min_vol;
	private String minvol_date;
	
	
	
	
	public CompositeKeyWritable(){
		
	}
	
	
	public CompositeKeyWritable(String s,Long max_v, String max_d, Long min_v, String min_d){
		this.symbol = s;
		this.max_vol=max_v;
		this.maxvol_date = max_d;
		this.min_vol = min_v;
		this.minvol_date = min_d;
	
		
		
	}
	
	
	public int compareTo(CompositeKeyWritable o) {
		int result =symbol.compareTo(o.symbol);
		if (result==0){
			result=max_vol.compareTo(o.max_vol);
		}
		
		return result;
		
	}
	

	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, symbol);
		WritableUtils.writeVLong(d, max_vol);
		WritableUtils.writeString(d, maxvol_date);
		WritableUtils.writeVLong(d, min_vol);
		WritableUtils.writeString(d, minvol_date);
		
		
	}

	public void readFields(DataInput di) throws IOException {
		symbol=WritableUtils.readString(di);
		max_vol=WritableUtils.readVLong(di);
		maxvol_date = WritableUtils.readString(di);
		min_vol=WritableUtils.readVLong(di);
		minvol_date = WritableUtils.readString(di);
		
		
	}


	
	public String getSymbol() {
		return symbol;
	}


	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	

	public Long getMaxVol() {
		return max_vol;
	}


	public void setMaxVol(Long max_vol) {
		this.max_vol = max_vol;
	}
	
	public String getMaxVolDate() {
		return maxvol_date;
	}

	public void setMaxVolDate(String maxvol_date) {
		this.maxvol_date = maxvol_date;
	}
	
	public Long getMinVol() {
		return min_vol;
	}


	public void setMinVol(Long min_vol) {
		this.min_vol = min_vol;
	}
	
	public String getMinVolDate() {
		return minvol_date;
	}

	public void setMinVolDate(String minvol_date) {
		this.minvol_date = minvol_date;
	}
	
	
	public String toString(){
		
		return (new StringBuilder().append(symbol).append("\t").append(max_vol).append("\t").append(maxvol_date).append("\t").toString());
		//return max_vol + "\t" + maxvol_date + "\t" + min_vol + "\t" + minvol_date + "\t" + price_adj ;
	}
	
	
	

}
