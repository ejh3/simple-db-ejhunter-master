package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int afIdx;
    private final int gbfIdx;
    private final Aggregator.Op op;
    private final Type gbfType;
    private final String gbfName;
    private final Aggregator aggregator;
    private final TupleDesc td;
    private OpIterator aggIter;
    private Boolean doneAggregating = false;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afIdx, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afIdx
     *            The column over which we are computing an aggregate.
     * @param gbfIdx
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param op
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afIdx, int gbfIdx, Aggregator.Op op) {
        this.child = child;
        this.afIdx = afIdx;
        this.gbfIdx = gbfIdx;
        this.op = op;
        if (gbfIdx == Aggregator.NO_GROUPING) {
            gbfType = null;
            gbfName = null;
        } else {
            gbfType = child.getTupleDesc().getFieldType(gbfIdx);
            gbfName = child.getTupleDesc().getFieldName(gbfIdx);
        }
        if (child.getTupleDesc().getFieldType(afIdx) == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gbfIdx, gbfType, afIdx, op);
        } else { // INT_TYPE
            aggregator = new IntegerAggregator(gbfIdx, gbfType, afIdx, op);
        }

        if (gbfIdx == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggregateFieldName()});
        } else {
            td = new TupleDesc(
                    new Type[]{gbfType, Type.INT_TYPE},
                    new String[]{groupFieldName(), aggregateFieldName()});
        }
        aggIter = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() { return gbfIdx; }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() { return gbfName; }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() { return afIdx; }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return String.format("%s(%s)", op.toString(), gbfType);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() { return op; }

    public static String nameOfAggregatorOp(Aggregator.Op op) { return op.toString(); }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(op) (child_td.getFieldName(afIdx))" where op and afIdx are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() { return td; }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
        if (aggIter != null) { aggIter.close(); }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggIter.rewind();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!doneAggregating) {
            while (child.hasNext())
                aggregator.mergeTupleIntoGroup(child.next());
            doneAggregating = true;
            aggIter.open();
        }

        if (aggIter.hasNext()) {
            Tuple ret = aggIter.next();
            ret.resetTupleDesc(td); // Resets to TupleDesc with same Types but different names.
            return ret;
        } else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() { return new OpIterator[]{child}; }

    @Override
    public void setChildren(OpIterator[] children) { child = children[0]; }
    
}
