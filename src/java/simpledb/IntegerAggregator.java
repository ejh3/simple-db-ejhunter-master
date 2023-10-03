package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    final private int gbfIdx;
    final private Type gbfType;
    final private int afIdx;
    final private Op op;
    final private Map<Field, Integer> aggregates = new HashMap<>(); // for SUM, MIN, MAX, or COUNT
    final private Map<Field, Integer> denominators = new HashMap<>(); // used only for AVG
    /**
     * Aggregate constructor
     * 
     * @param gbfIdx the 0-based index of the group-by field in the tuple, or
     *               NO_GROUPING if there is no grouping
     * @param gbfType the type of the group by field (e.g., Type.INT_TYPE), or null
     *                if there is no grouping
     * @param afIdx the 0-based index of the aggregate field in the tuple
     * @param op the aggregation operator
     * @throws IllegalArgumentException if (gbfIdx == NO_GROUPING) != (gbfType == null)
     */

    public IntegerAggregator(int gbfIdx, Type gbfType, int afIdx, Op op) {
        if ((gbfIdx == NO_GROUPING) != (gbfType == null)) {
            throw new IllegalArgumentException("gbfIdx and gbfType are inconsistent!");
        }
        this.gbfIdx = gbfIdx;
        this.gbfType = gbfType;
        this.afIdx = afIdx;
        this.op = op;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbf = (gbfIdx == NO_GROUPING ? null : tup.getField(gbfIdx));
        if (gbf != null && gbf.getType() != gbfType) {
            throw new IllegalArgumentException(String.format("Supplied tuple's GROUP BY field " +
                    "has type %s which is inconsistent with expected type %s.",
                    gbf.getType(), gbfType));
        }
        if (tup.getField(afIdx).getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("An IntegerAggregator cannot work on non-integer " +
                    "types.");
        }
        IntField af = (IntField) tup.getField(afIdx);
        if (op == Op.MIN) { aggregates.merge(gbf, af.getValue(), Math::min); }
        else if (op == Op.MAX) { aggregates.merge(gbf, af.getValue(), Math::max); }
        else if (op == Op.SUM) { aggregates.merge(gbf, af.getValue(), Integer::sum); }
        else if (op == Op.AVG) { aggregates.merge(gbf, af.getValue(), Integer::sum);
                                 denominators.merge(gbf, 1, (oldVal, newVal) -> oldVal + 1); }
        else { // (op == Op.COUNT)
            aggregates.merge(gbf, 1, (oldVal, newVal) -> oldVal + 1);
        }
    }

    /**
     * Create an OpIterator over group aggregate results.
     * 
     * @return an OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        //noinspection DuplicatedCode
        return new OpIterator() {
            private Boolean open = false;
            private Iterator<Field> groupIter;
            private Tuple next;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                groupIter = aggregates.keySet().iterator();
                if (gbfIdx == NO_GROUPING) {
                    next = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
                } else {
                    next = new Tuple(new TupleDesc(new Type[]{gbfType, Type.INT_TYPE}));
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!this.open)
                    throw new IllegalStateException("Operator not yet open");

                return groupIter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException,
                    NoSuchElementException {
                if (!hasNext()) { // Also checks if open.
                    throw new NoSuchElementException();
                }
                Field group = groupIter.next();
                IntField aggregate;
                if (op == Op.AVG) {
                    aggregate = new IntField(aggregates.get(group) / denominators.get(group));
                } else { // MIN, MAX, SUM, COUNT
                    aggregate = new IntField(aggregates.get(group));
                }
                if (gbfIdx == NO_GROUPING) {
                    next.setFields(aggregate);
                } else {
                    next.setFields(group, aggregate);
                }
                return next;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                groupIter = aggregates.keySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return next.getTupleDesc();
            }

            @Override
            public void close() {
                open = false;
            }
        };
    }

}
