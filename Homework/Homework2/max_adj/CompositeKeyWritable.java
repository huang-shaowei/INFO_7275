package max_adj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable>  {

	
	private String symbol;
	private Double max_adj;
	
	
	
	
	public CompositeKeyWritable(){
		
	}
	
	
	public CompositeKeyWritable(String s,Double max_adj){
		this.symbol = s;
		this.max_adj=max_adj;
		
	}
	
	
	public int compareTo(CompositeKeyWritable o) {
		int result =symbol.compareTo(o.symbol);
		if (result==0){
			result=max_adj.compareTo(o.max_adj);
		}
		
		return result;
		
	}
	

	public void write(DataOutput d) throws IOException {
		d.writeUTF(symbol);
		d.writeDouble(max_adj);

		
	}

	public void readFields(DataInput di) throws IOException {
		this.symbol=di.readUTF();
		this.max_adj=di.readDouble();

		
		
	}


	
	public String getSymbol() {
		return symbol;
	}


	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	

	public Double getMaxAdj() {
		return max_adj;
	}


	public void setMaxAdj(Double max_adj) {
		this.max_adj = max_adj;
	}
	
	
	public String toString(){
		
		return (new StringBuilder().append(symbol).append("\t").append(max_adj).toString());
		//return max_vol + "\t" + maxvol_date + "\t" + min_vol + "\t" + minvol_date + "\t" + price_adj ;
	}
	
	
	

}
