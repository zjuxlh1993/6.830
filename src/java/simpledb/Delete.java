package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    OpIterator child;
    TransactionId tid;
    int tableId;
    private static final TupleDesc td = Utility.getTupleDesc(1);
    boolean isopen = false;
    Tuple tuple;
    int count;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.child = child;
    	this.tid = t;
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
				Database.getBufferPool().deleteTuple(tid, child.next());
				++count;
			} catch (NoSuchElementException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	child.close();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
