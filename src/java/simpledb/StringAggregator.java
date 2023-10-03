package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfIdx;
    private final Type gbfType;
    private final int afIdx;
    private final Map<Field, Integer> counts = new HashMap<>();
    /**
     * Aggregate constructor
     * @param gbfIdx the 0-based index of the group-by field in the tuple, or
     *               NO_GROUPING if there is no grouping
     * @param gbfType the type of the group by field (e.g., Type.INT_TYPE), or null
     *                if there is no grouping
     * @param afIdx the 0-based index of the aggregate field in the tuple
     * @param op aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if (gbfIdx == NO_GROUPING) != (gbfType == null) or
     *                                  if op != COUNT
     */

    public StringAggregator(int gbfIdx, Type gbfType, int afIdx, Op op) {
        if ((gbfIdx == NO_GROUPING) != (gbfType == null))
            throw new IllegalArgumentException("gbfIdx and gbfType are inconsistent!");
        if (op != Op.COUNT)
            throw new IllegalArgumentException("COUNT is the only valid aggregate for strings");

        this.gbfIdx = gbfIdx;
        this.gbfType = gbfType;
        this.afIdx = afIdx;
        Op op1 = Op.COUNT;
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
        if (tup.getField(afIdx).getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException("A StringAggregator cannot work on non-string " +
                    "types.");
        }
        StringField af = (StringField) tup.getField(afIdx);
        counts.merge(gbf, 1, (oldVal, newVal) -> oldVal + 1);
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
                groupIter = counts.keySet().iterator();
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
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Field group = groupIter.next();
                IntField count;
                count = new IntField(counts.get(group));

                if (gbfIdx == NO_GROUPING) {
                    next.setFields(count);
                } else {
                    next.setFields(group, count);
                }
                return next;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                groupIter = counts.keySet().iterator();
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
