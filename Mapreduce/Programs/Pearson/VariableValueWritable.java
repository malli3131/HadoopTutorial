package com.pearson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * A pair of indices corresponding to the i-th and j-th variables/columns.
 * 
 * @author Nagamallikarjuna
 * 
 */
public class VariableValueWritable implements
		WritableComparable<VariableValueWritable> {

	private double i;
	private double j;

	/**
	 * Constructor.
	 */
	public VariableValueWritable() {
		this(0, 0);
	}

	/**
	 * Constructor.
	 * 
	 * @param i
	 *            i-th index.
	 * @param j
	 *            j-th index.
	 */
	public VariableValueWritable(double i, double j) {
		this.i = i;
		this.j = j;
	}

	@Override
	public boolean equals(Object object) {
		if (null == object)
			return false;
		if (!(object instanceof VariableValueWritable))
			return false;
		VariableValueWritable indexPairs = (VariableValueWritable) object;
		double i1 = getI();
		double j1 = getJ();
		double i2 = indexPairs.getI();
		double j2 = indexPairs.getJ();

		return (i1 == i2 && j1 == j2);
	}

	@Override
	public int hashCode() {
		return 37 + (new Double(i)).hashCode() + (new Double(j)).hashCode();
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append('{').append(getI()).append(',')
				.append(getJ()).append('}').toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readDouble();
		j = in.readDouble();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(getI());
		out.writeDouble(getJ());
	}

	/**
	 * Gets the i-th variable index.
	 * 
	 * @return Double.
	 */
	public double getI() {
		return i;
	}

	/**
	 * Gets the j-th variable index.
	 * 
	 * @return Double.
	 */
	public double getJ() {
		return j;
	}

	/**
	 * Sets the i-th variable index.
	 * 
	 * @param i
	 *            Double.
	 */
	public void setI(double i) {
		this.i = i;
	}

	/**
	 * Sets the j-th variable index.
	 * 
	 * @param j
	 */
	public void setJ(double j) {
		this.j = j;
	}

	@Override
	public int compareTo(VariableValueWritable object) {
		Double i1 = getI();
		Double j1 = getJ();
		Double i2 = object.getI();
		Double j2 = object.getJ();

		int result = i1.compareTo(i2);
		if (0 == result) {
			return j1.compareTo(j2);
		}

		return result;
	}

}
