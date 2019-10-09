package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbField, aField;
    Type gbfieldType;
    Op op;
    boolean isGrouping;
    HashMap<Field, Tuple> aggregateRes = new HashMap<Field, Tuple>();
    TupleDesc aggregateDesc;
    Vector<Tuple> tuples;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	gbField = gbfield;
    	gbfieldType = gbfieldtype;
    	aField = afield;
    	op = what;
    	isGrouping = gbfield == NO_GROUPING?false:true;
    	Type[] typeAr;
    	if (isGrouping) {
    		typeAr = new Type[] {gbfieldtype, Type.INT_TYPE};
    	} else typeAr = new Type[] {Type.INT_TYPE};  		
    	aggregateDesc = new TupleDesc(typeAr); 
    }

    private int getAValue(Tuple tuple) {
    	return ((IntField)(tuple.getField(isGrouping?1:0))).getValue();
    }
    
    private Tuple merge(Tuple t1, Tuple t2) {
		if (t2 == null) {
			t2 = new Tuple(aggregateDesc);
			if (isGrouping) t2.setField(0, t1.getField(gbField));
			t2.setField(isGrouping?1:0, new IntField(0));
			aggregateRes.put(t1.getField(gbField), t2);
		}
		
    	switch (op) {
		case COUNT:
			t2.setField(isGrouping?1:0, new IntField(getAValue(t2)+1));
			break;
		default:
			throw new UnsupportedOperationException();
		}
    	return t2;
    }
    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field field = tup.getField(gbField);
    	if (isGrouping) {

    		aggregateRes.put(field, merge(tup, aggregateRes.get(field)));
    	} else {
    		aggregateRes.put(null, merge(tup, aggregateRes.get(field)));
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	return new OpIterator() {		
			private static final long serialVersionUID = 1L;
			
			Iterator<Field> iter;
    		boolean isOpen;
    		
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				iter = aggregateRes.keySet().iterator();
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				isOpen = true;
				rewind();
				
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				// TODO Auto-generated method stub
				if (!isOpen)
					throw new IllegalStateException();
				if (!iter.hasNext())
					throw new NoSuchElementException();
				return aggregateRes.get(iter.next());
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if (!isOpen)
					throw new IllegalStateException();
				return iter.hasNext();
			}
			
			@Override
			public TupleDesc getTupleDesc() {
				return aggregateDesc;
			}
			
			@Override
			public void close() {
				isOpen = false;
				iter = null;
			}
		};
    }

}
