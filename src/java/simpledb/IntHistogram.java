package simpledb;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private class BucketInfo {
		int sum, num;
		void addValue(int v) {
			sum += v;
			++num;
		}
		
		int getNum() {
			return num;
		}
		
		public BucketInfo() {
			sum = 0;
			num = 0;
		}
	}
	
	int min,max,totalNum,bs;
	double interval;
	
	private BucketInfo[] buckets;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	bs = buckets;
    	this.buckets = new BucketInfo[buckets];
    	for (int i=0;i!=buckets;i++) this.buckets[i]= new BucketInfo(); 
    	this.min = min;
    	this.max = max + 1;
    	interval = (double)(this.max - this.min) / buckets;
    	totalNum = 0;
    }

    private double cal(int v) {
    	if (v > max-1) return 0;
    	double sum = 0;
    	for (int i=(int)Math.floor((v-min)/interval); i!=bs; i++) {
    		sum += buckets[i].getNum();
    	}
    	return sum;
    }
    
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	buckets[(int)Math.floor((v-min)/interval)].addValue(v);
    	++totalNum;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	if (v < min) v = min;
    	// some code goes here
    	switch (op) {
		case EQUALS:
			return (double)buckets[(int)Math.floor((v-min)/interval)].getNum()/interval/totalNum;
		case NOT_EQUALS:
			return 1.0 - (double)buckets[(int)Math.floor((v-min)/interval)].getNum()/interval/totalNum;
		case GREATER_THAN:
			return cal(v) / totalNum;
		case GREATER_THAN_OR_EQ:
			return cal(v) / totalNum;
		case LESS_THAN:
			return 1.0 - estimateSelectivity(op.GREATER_THAN_OR_EQ, v);
		case LESS_THAN_OR_EQ:
			return 1.0 - estimateSelectivity(op.LESS_THAN, v);
		default:
			break;
		}
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
