package simpledb;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	File file;
	TupleDesc tDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	tDesc = td;
    	file = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return tDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	RandomAccessFile randomAccessFile;
    	try {
			randomAccessFile = new RandomAccessFile(file, "rw");
			int pageSize = BufferPool.getPageSize();
			byte[] dataBytes = new byte[pageSize];
			randomAccessFile.seek(pageSize * pid.getPageNumber());
			randomAccessFile.readFully(dataBytes);
			randomAccessFile.close();
			return new HeapPage((HeapPageId)pid, dataBytes);
		} catch (IOException e) {
			throw new IllegalArgumentException();
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
			int pgNo = 0;
			Iterator<Tuple> it = null;
			
			private Iterator<Tuple> getIt() throws TransactionAbortedException, DbException {
				if (pgNo < numPages())
					return ((HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo++), Permissions.READ_WRITE))).iterator();
				else
					return null;
			}
			
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				pgNo = 0;
				it = getIt();
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				rewind();
				
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (hasNext())
					return it.next();
				throw new NoSuchElementException();
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if (it == null)
					return false;
				if (!it.hasNext()) {
					return (it = getIt()) != null;
				} else return true;
			}
			
			@Override
			public void close() {
				it = null;
			}
		};
    }

}

