package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.LongDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps a user-facing Schema with two trailing system columns that hold
 * each row's MVCC version stamps: __xmin, __xmax (both Long).
 *
 * Conventions:
 *   - "user" Schema/Record exposes only the columns the application defined.
 *   - "physical" Schema/Record carries the user columns plus the two stamps.
 *   - The stamps live at the LAST two positions; appending (rather than
 *     prepending) keeps user column indexes stable and preserves any
 *     existing serialized layouts.
 *
 * xmax == 0 is the sentinel for "live, not deleted".
 */
public final class MvccSchema {

    public static final String XMIN = "__xmin";
    public static final String XMAX = "__xmax";
    public static final long ALIVE = 0L;

    private MvccSchema() {}

    /** Build the on-disk schema by appending xmin/xmax columns to the user schema. */
    public static Schema physical(Schema userSchema) {
        Schema out = new Schema();
        for (int i = 0; i < userSchema.size(); i++) {
            out.add(userSchema.getFieldName(i), userSchema.getFieldType(i));
        }
        out.add(XMIN, Type.longType());
        out.add(XMAX, Type.longType());
        return out;
    }

    /** Build a physical Record by stamping (xmin, xmax) onto a user Record. */
    public static Record attachVersion(Record userRecord, long xmin, long xmax) {
        List<DataBox> values = new ArrayList<>(userRecord.getValues());
        values.add(new LongDataBox(xmin));
        values.add(new LongDataBox(xmax));
        return new Record(values);
    }

    /** Strip the trailing stamps from a physical Record to recover the user view. */
    public static Record userRecord(Record physical) {
        List<DataBox> all = physical.getValues();
        return new Record(all.subList(0, all.size() - 2));
    }

    public static long xmin(Record physical) {
        return physical.getValue(physical.size() - 2).getLong();
    }

    public static long xmax(Record physical) {
        return physical.getValue(physical.size() - 1).getLong();
    }

    /** Return a copy of physical with xmax replaced. xmin and user columns unchanged. */
    public static Record withXmax(Record physical, long newXmax) {
        List<DataBox> values = new ArrayList<>(physical.getValues());
        values.set(values.size() - 1, new LongDataBox(newXmax));
        return new Record(values);
    }
}
