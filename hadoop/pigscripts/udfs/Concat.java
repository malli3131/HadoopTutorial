package malli.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Concat extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0)
		{
			return null;
		}
		String fullName = "";
		String firstName = (String) input.get(0);
		String lastName = (String) input.get(1);
		fullName = firstName + " " + lastName;
		return fullName;
	}

}
