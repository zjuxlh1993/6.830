package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NOGROUP_FIELD = null;

    int gbField, aField;
    Type gbfieldType;
    Op op;
    boolean isGrouping;
    HashMap<Field, entry> aggregateRes = new HashMap<Field, IntegerAggregator.entry>();
    TupleDesc aggregateDesc;
    Vector<Tuple> tuples;
    
    private class entry {
    	Tuple tuple;
    	int v;
    	int s;
    	public entry(Tuple t, int s) {
			tuple = t;
			v = 1;
			this.s = s;
		}
    }
    
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
    	return ((IntField)(tuple.getField(aField))).getValue();
    }
    
    private int getEAValue(entry e) {
    	return ((IntField)(e.tuple.getField(isGrouping?1:0))).getValue();
    }
    
    public entry merge(Tuple tup, entry e) {
    	if (e == null) {
    		entry entry = getTuple(tup);
    		if (op == Op.COUNT) {
    			entry.tuple.setField(isGrouping?1:0, new IntField(1));
    		}
    		return entry;
    	}
    	switch (op) {
		case COUNT:
			e.tuple.setField(isGrouping?1:0, new IntField(getEAValue(e)+1));
			break;
		case MAX:
			e.tuple.setField(isGrouping?1:0, new IntField(Math.max(getEAValue(e), getAValue(tup))));
			break;
		case MIN:
			e.tuple.setField(isGrouping?1:0, new IntField(Math.min(getEAValue(e), getAValue(tup))));
			break;
		case SUM:
			e.tuple.setField(isGrouping?1:0, new IntField(getEAValue(e) + getAValue(tup)));
			break;
		case AVG:
			++e.v;
			e.s += getAValue(tup);
			e.tuple.setField(isGrouping?1:0, new IntField(e.s / e.v));
			break;
		default:
			break;
		}
    	return e;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (isGrouping) {
    		Field field = tup.getField(gbField);
    		aggregateRes.put(field, merge(tup, aggregateRes.get(field)));
    	} else {
    		aggregateRes.put(NOGROUP_FIELD, merge(tup, aggregateRes.get(NOGROUP_FIELD)));
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	return new OpIterator() {		
    		/**
			 * 
			 */
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
				return aggregateRes.get(iter.next()).tuple;
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
    
    /**
     *  Get a new tuple contain gbfield and afield
     *  
     *  @param tup
     *             the Tuple containing an aggregate field and a group-by field
     */
    
    private entry getTuple(Tuple tup) {
    	int index = 0;
    	Tuple reTuple = new Tuple(aggregateDesc);
    	if (isGrouping) {
    		reTuple.setField(index++, tup.getField(gbField));
    	}
    	reTuple.setField(index, tup.getField(aField));
    	return new entry(reTuple, getAValue(tup));
    }
}
