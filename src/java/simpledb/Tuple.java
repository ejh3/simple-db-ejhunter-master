package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId rid = null;
    private final List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.td = td;
        fields = new ArrayList<>();
        for (int i = 0; i < this.td.numFields(); i++) {
            fields.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() { return td; }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() { return rid; }

    /**
     * @return The tableid for the table this tuple is on
     */
    public int getTableId() { return rid.getPageId().getTableId(); }

    /**
     * @return The PageId associated with this tuple
     */
    public PageId getPageId() { return rid.getPageId(); }

    /**
     * @return The number of the page that this tuple is on
     */
    public int getPageNumber() { return rid.getPageId().getPageNumber(); }

    /**
     * @return Which tuple this is on the page.
     */
    public int getTupleNumber() { return rid.getTupleNumber(); }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field. Must be compatible with corresponding TupleDesc.
     */
    public void setField(int i, Field f) {
        if (td.getFieldType(i) != f.getType()) {
            throw new IllegalArgumentException("Field f has type incompatible with the TupleDesc.");
        }
        fields.set(i, f);
    }

    /**
     * Change the value of the multiple fields in this tuple, starting with idx.
     *
     * @param idx
     *            index of the first field to change. It must be a valid index.
     * @param fs
     *            new values for the fields, starting at index idx. Must be compatible
     *            with this Tuple's TupleDesc.
     */
    public void setFields(int idx, Field... fs) {
        for (int i = 0; i < fs.length; i++)
            fields.set(i + idx, fs[i]);
    }

    /**
     * Change the value of the multiple fields in this tuple, starting with field 0.
     *
     * @param fs
     *            new values for the fields, starting at index 0. Must be compatible
     *            with this Tuple's TupleDesc.
     */
    public void setFields(Field... fs) {
        setFields(0, fs);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (Field field : fields) {
            s.append("\t").append(field);
        }
        return s.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = td;
    }

    /**
     * Merge two Tuples into one, with t1 coming first. td must have the
     * appropriate number and type of fields to perfectly all of t1 and t2.
     *
     * @param t1
     *            The Tuple with the first fields of the new Tuple
     * @param t2
     *            The Tuple with the last fields of the new Tuple
     * @param td
     *            The TupleDesc of the new Tuple
     * @return the new Tuple
     */
    public static Tuple merge(Tuple t1, Tuple t2, TupleDesc td) {
        Tuple ret = new Tuple(td);
        ret.fields.clear();
        ret.fields.addAll(t1.fields);
        ret.fields.addAll(t2.fields);
        if (ret.fields.size() != td.numFields()) {
            throw new IllegalArgumentException("Mismatch between size of Tuples and TupleDesc." +
                    " Cannot merge.");
        }
        return ret;
    }
}
