package twok.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * This data type used for both key and value. It is implementing WritableComparable.
 * 
 **/
import org.apache.hadoop.io.WritableComparable;

public class PersonWritable implements WritableComparable<PersonWritable>{
	String firstName;
	String lastName;
	
	public void readFields(DataInput input) throws IOException {
		firstName = input.readUTF();
		lastName = input.readUTF();
	}

	public void write(DataOutput output) throws IOException {
		output.writeUTF(firstName);
		output.writeUTF(lastName);
	}
	
	public void setFirstName(String fname) 
	{ 
		this.firstName = fname;
	}
	
	public void setLastName(String lname) 
	{ 
		this.lastName = lname;
	}	

	  /** Return the value of this IntWritable. */
	  public String getFirstName() 
	  { 
		  return firstName; 
	  }
	  public String getLastName() 
	  { 
		  return lastName; 
	  }

	@Override
	public int compareTo(PersonWritable person) {
		return (firstName.compareTo(person.firstName) != 0) ? firstName.compareTo(person.firstName) : lastName.compareTo(person.lastName);
	}
}
