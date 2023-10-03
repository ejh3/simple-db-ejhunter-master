package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private boolean insertionCompleted = false;

    /**
     * Constructor.
     *
     * @param tid
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId tid, OpIterator child, int tableId)
            throws DbException {
        this.tid = tid;
        this.child = child;
        this.tableId = tableId;
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        if (!table.getTupleDesc().equals(child.getTupleDesc()))
            throw new DbException("Cannot insert into table with different TupleDesc");
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: maybe change this to throw DbException? Not sure that rewind should be supported
        // on this. Change Delete too if you change it.
        insertionCompleted = false;
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instance of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: maybe catch DbExceptions to correctly account for failed inserts or deletes
        if (insertionCompleted) { return null; }
        int i = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
                i++;
            } catch (IOException e) {
                throw new DbException(e + "Could not write to file for some reason");
            }
        }
        insertionCompleted = true;
        Tuple numInserted = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        numInserted.setFields(new IntField(i));
        return numInserted;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{ child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
