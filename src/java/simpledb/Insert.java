package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    OpIterator child;
    TransactionId tid;
    int tableId;
    private static final TupleDesc td = Utility.getTupleDesc(1);
    boolean isopen = false;
    int count;
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.tableId = tableId;
    	this.child = child;
    	this.tid = t;
    	if (!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc()))
    		throw new DbException("TupleDesc of child differs from table");
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	rewind();
    }

    public void close() {
        // some code goes here
    	super.close();
    	isopen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	count = 0;
    	isopen = true;
    	while (child.hasNext()) {
    		try {
				Database.getBufferPool().insertTuple(tid, tableId, child.next());
				++count;
			} catch (NoSuchElementException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	child.close();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (!isopen)
    		return null;
    	isopen = false;
    	Tuple tuple = new Tuple(td);
    	tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child = children[0];
    }
}
