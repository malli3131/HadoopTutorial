package twok.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PersonWritable implements Writable{

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */

	String name;
	String location;
	String company;
	public void readFields(DataInput input) throws IOException {
		name = input.readUTF();
		location = input.readUTF();
		company = input.readUTF();
	}

	public void write(DataOutput output) throws IOException {
		output.writeUTF(name);
		output.writeUTF(location);
		output.writeUTF(company);
	}
	
	public void setName(String name) 
	{ 
		this.name = name;
	}
	
	public void setLocation(String location) 
	{ 
		this.location = location;
	}
	
	public void setCompany(String company) 
	{ 
		this.company = company;
	}
	

	  /** Return the value of this IntWritable. */
	  public String getName() 
	  { 
		  return name; 
	  }
	  public String getLoc() 
	  { 
		  return location; 
	  }
	  public String getCompany() 
	  { 
		  return company; 
	  }
}
