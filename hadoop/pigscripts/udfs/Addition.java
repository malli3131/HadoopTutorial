package malli.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Addition extends EvalFunc<Integer>{

	@Override
	public Integer exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0)
		{
			return null;
		}
		Integer number1 = (Integer) input.get(0);
		Integer number2 = (Integer) input.get(1);
		Integer sum = number1 + number2;
		return sum;
	}
}
