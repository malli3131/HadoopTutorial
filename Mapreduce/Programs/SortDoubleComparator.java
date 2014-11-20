/*
This is a custom sort comparator to change the default hadoop sorting order from ASC to DESC. call this java class in your
MapReduce method to alter the job output in desc order using the follwoing method on job object.
i.e job.setSortComparatorClass(DoubleComparator.class);
*/
public static class DoubleComparator extends WritableComparator {
	    public DoubleComparator() {
	      super(DoubleWritable.class);
	    }

	    private Double num1;
	    private Double num2;

	    @Override
	    public int compare(byte[] raw1, int offset1, int length1, byte[] raw2,
	        int offset2, int length2) {
	      num1 = ByteBuffer.wrap(raw1, offset1, length1).getDouble();
	      num2 = ByteBuffer.wrap(raw2, offset2, length2).getDouble();

	      return num2.compareTo(num1);
	    }
	  }
