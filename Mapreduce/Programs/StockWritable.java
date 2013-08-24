package twok.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StockWritable implements Writable {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */
	
	double open = 0.0;
	double high = 0.0;
	double low = 0.0;
	double close = 0.0;
	long volume = 0L;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.open = in.readDouble();
		this.high = in.readDouble();
		this.low = in.readDouble();
		this.close = in.readDouble();
		this.volume = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(open);
		out.writeDouble(high);
		out.writeDouble(low);
		out.writeDouble(close);
		out.writeLong(volume);
	}
	
	//setter and getter Methods...
	public void setOpen(Double stockOpen)
	{
		open = stockOpen;
	}
	public Double getOpen()
	{
		return open;
	}
	public void setHigh(Double stockHigh)
	{
		high = stockHigh;
	}
	public Double getHigh()
	{
		return high;
	}
	public void setLow(Double stockLow)
	{
		low = stockLow;
	}
	public Double getLow()
	{
		return low;
	}
	public void setClose(Double stockClose)
	{
		close = stockClose;
	}
	public Double getClose()
	{
		return close;
	}
	public void setVolume(Long stockVolume)
	{
		volume = stockVolume;
	}
	public Long getVolume()
	{
		return volume;
	}
}
