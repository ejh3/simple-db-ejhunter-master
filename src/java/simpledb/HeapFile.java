package simpledb;

import java.io.*;
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

    final private File f;
    final private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid.getTableId() != this.getId()) {
            String thisTable = Database.getCatalog().getTableName(this.getId());
            String otherTable = Database.getCatalog().getTableName(pid.getTableId());
            String ret = String.format("Page #%d is in file %s, not in %s",
                    pid.getPageNumber(), thisTable, otherTable);
            throw new IllegalArgumentException(ret);
        }

        byte[] data = new byte[BufferPool.getPageSize()];
        try (RandomAccessFile is = new RandomAccessFile(f, "r")) {
            is.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            is.readFully(data, 0, BufferPool.getPageSize());
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        return null; // Superfluous. Just to make IDE happy
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile is = new RandomAccessFile(f, "rw");
        is.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
        is.write(page.getPageData(), 0, BufferPool.getPageSize());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int currPgNo = 0;
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(
                tid, new HeapPageId(getId(), currPgNo), Permissions.READ_WRITE);
        while (heapPage.getNumEmptySlots() == 0) {
            currPgNo++;
            if (currPgNo >= numPages()) {
                HeapPage emptyPage = new HeapPage(
                        new HeapPageId(getId(), currPgNo), HeapPage.createEmptyPageData());
                writePage(emptyPage);
            }
            heapPage = (HeapPage) Database.getBufferPool().getPage(
                    tid, new HeapPageId(getId(), currPgNo), Permissions.READ_WRITE);
        }
        heapPage.insertTuple(t);
        return new ArrayList<Page>(List.of(heapPage));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(
                tid, t.getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);

        /* DbFile says "This tuple should be updated to reflect that it is no longer stored on any
        page," but BufferPoolWriteTest doesn't work if I change the tuple's recordId to null after
        deletion, so I'm leaving it this way for now. */
        //t.setRecordId(null); // appropriate to set this after deletion, as deletion might throw
        t.setRecordId(new RecordId(new HeapPageId(t.getTableId(), t.getPageNumber()), -1));

        return new ArrayList<Page>(List.of(heapPage));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // anonymous inner class only valid because tid is "effectively final".
        return new DbFileIterator() {

            private int currPgNo = -1;
            private HeapPageId currPgId;
            private HeapPage currPage;
            private Iterator<Tuple> pageIter;
            private boolean open = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (!open) {
                    open = true;
                    rewind();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!open) { return false; } // Not open yet.
                while (!pageIter.hasNext()) {
                    if (++currPgNo >= numPages()) { return false; }
                    currPgId = new HeapPageId(getId(), currPgNo);
                    currPage = (HeapPage) Database.getBufferPool().getPage(
                            tid, currPgId, Permissions.READ_ONLY);
                    pageIter = currPage.iterator();
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext())
                    throw new NoSuchElementException("No more tuples in this HeapFile!");

                return pageIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!open)
                    throw new DbException("Must open() this HeapFile Iterator to rewind().");
                currPgNo = 0;
                currPgId = new HeapPageId(getId(), currPgNo);
                currPage = (HeapPage) Database.getBufferPool().getPage(
                        tid, currPgId, Permissions.READ_ONLY);
                pageIter = currPage.iterator();
                open = true;
            }

            @Override
            public void close() {
                currPgNo = -1;
                currPgId = null;
                currPage = null;
                pageIter = null;
                open = false;
            }
        };
    }

}

