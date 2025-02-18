package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    JoinPredicate joinPredicate;
    OpIterator child1;
    OpIterator child2;
    TupleDesc mergeDesc;
    Iterator<Tuple> it;
    private ArrayList<Tuple> childTups = new ArrayList<Tuple>();

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
    	joinPredicate = p;
    	this.child1 = child1;
    	this.child2 = child2;
    	mergeDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return mergeDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	child1.open();
        child2.open();
        while (child1.hasNext()) {
        	Tuple tmpTuple1 = child1.next();
        	while (child2.hasNext()) {
        		Tuple tmpTuple2 = child2.next();
        		if (joinPredicate.filter(tmpTuple1, tmpTuple2)) {
            		Tuple merTuple = new Tuple(mergeDesc);
            		int len1 = child1.getTupleDesc().numFields();
            		int len2 = child2.getTupleDesc().numFields();
            		for (int i=0; i!=len1; i++) {
            			merTuple.setField(i, tmpTuple1.getField(i));
            		}
            		for (int i=len1; i!=len1+len2; i++) {
            			merTuple.setField(i, tmpTuple2.getField(i-len1));
            		}
            		childTups.add(merTuple);
            	}
        	}
        	child2.rewind();
        }
        it = childTups.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	it  = childTups.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child1 = children[0];
    	child2 = children[1];
    }

}
