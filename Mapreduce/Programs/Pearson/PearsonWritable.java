package com.pearson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Holds intermediary results of Pearson correlation.
 * 
 * @author Nagamallikarjuna
 * 
 */
public class PearsonWritable implements Writable {

	private double xy;
	private double x;
	private double y;
	private double n;
	private double xx;
	private double yy;

	/**
	 * Constructor.
	 */
	public PearsonWritable() {
		this(0.0d, 0.0d, 0.0d, 0.0d, 0.0d, 0.0d);
	}

	/**
	 * Constructor.
	 * 
	 * @param x
	 *            Sum of x.
	 * @param y
	 *            Sum of y.
	 * @param xx
	 *            Sum of x * y.
	 * @param yy
	 *            Sum of y * y.
	 * @param xy
	 *            Sum of x * y.
	 * @param n
	 *            Total data points.
	 */
	public PearsonWritable(double x, double y, double xx, double yy, double xy,
			double n) {
		this.x = x;
		this.y = y;
		this.xx = xx;
		this.yy = yy;
		this.xy = xy;
		this.n = n;
	}

	@Override
	public int hashCode() {
		return 37 + (new Double(x)).hashCode() + (new Double(y)).hashCode()
				+ (new Double(xx)).hashCode() + (new Double(yy)).hashCode()
				+ (new Double(xy)).hashCode() + (new Double(n)).hashCode();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
		xx = in.readDouble();
		yy = in.readDouble();
		xy = in.readDouble();
		n = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(getX());
		out.writeDouble(getY());
		out.writeDouble(getXX());
		out.writeDouble(getYY());
		out.writeDouble(getXY());
		out.writeDouble(getN());
		out.writeDouble(getCorrelation());
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append('{').append(x).append(',')
				.append(y).append(',').append(xx).append(',').append(yy)
				.append(',').append(xy).append(',').append(n).append(',')
				.append(getCorrelation()).append('}').toString();
	}

	/**
	 * Gets the computed Pearson correlation.
	 * 
	 * @return Pearson correlation.
	 */
	public double getCorrelation() {
		if (0.0d == n)
			return 0.0d;

		double numerator = x / n;
		numerator = numerator * y;
		numerator = xy - numerator;

		double denom1 = Math.pow(x, 2.0d) / n;
		denom1 = xx - denom1;

		double denom2 = Math.pow(y, 2.0d) / n;
		denom2 = yy - denom2;

		double denom = denom1 * denom2;
		denom = Math.sqrt(denom);

		double r = numerator / denom;
		return r;
	}

	/**
	 * Gets sum of x * y.
	 * 
	 * @return double.
	 */
	public double getXY() {
		return xy;
	}

	/**
	 * Sets sum of x * y.
	 * 
	 * @param xy
	 *            double.
	 */
	public void setXY(double xy) {
		this.xy = xy;
	}

	/**
	 * Gets sum of x.
	 * 
	 * @return double.
	 */
	public double getX() {
		return x;
	}

	/**
	 * Sets sum of x.
	 * 
	 * @param x
	 *            double.
	 */
	public void setX(double x) {
		this.x = x;
	}

	/**
	 * Gets sum of y.
	 * 
	 * @return double.
	 */
	public double getY() {
		return y;
	}

	/**
	 * Sets sum of y.
	 * 
	 * @param y
	 *            double.
	 */
	public void setY(double y) {
		this.y = y;
	}

	/**
	 * Gets total.
	 * 
	 * @return double.
	 */
	public double getN() {
		return n;
	}

	/**
	 * Sets total.
	 * 
	 * @param n
	 *            double.
	 */
	public void setN(double n) {
		this.n = n;
	}

	/**
	 * Gets sum of x * x.
	 * 
	 * @return double.
	 */
	public double getXX() {
		return xx;
	}

	/**
	 * Sets sum of x * x.
	 * 
	 * @param xx
	 *            double.
	 */
	public void setXX(double xx) {
		this.xx = xx;
	}

	/**
	 * Gets sum of y * y.
	 * 
	 * @return double.
	 */
	public double getYY() {
		return yy;
	}

	/**
	 * Sets sum of y * y.
	 * 
	 * @param yy
	 *            double.
	 */
	public void setYY(double yy) {
		this.yy = yy;
	}

}
