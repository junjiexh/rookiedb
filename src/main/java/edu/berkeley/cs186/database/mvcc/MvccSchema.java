package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.LongDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps a user-facing Schema with three trailing system columns that carry
 * each row's MVCC metadata: __xmin, __xmax, __next_rid.
 *
 * Layout indices (counted from end):
 *   size-1: __next_rid   (encoded RecordId; NO_NEXT = -1)
 *   size-2: __xmax       (0 = alive)
 *   size-3: __xmin
 *
 * Stamps are appended so user column indexes are stable. xmax and next_rid
 * are set together when a version is superseded: xmax = updater's xid and
 * next_rid = the new version's RecordId. For deletes, xmax is stamped but
 * next_rid stays NO_NEXT.
 */
public final class MvccSchema {

    public static final String XMIN = "__xmin";
    public static final String XMAX = "__xmax";
    public static final String NEXT = "__next_rid";
    public static final long ALIVE = 0L;
    public static final long NO_NEXT = -1L;

    private MvccSchema() {}

    /** Build the on-disk schema by appending xmin/xmax/next columns. */
    public static Schema physical(Schema userSchema) {
        Schema out = new Schema();
        for (int i = 0; i < userSchema.size(); i++) {
            out.add(userSchema.getFieldName(i), userSchema.getFieldType(i));
        }
        out.add(XMIN, Type.longType());
        out.add(XMAX, Type.longType());
        out.add(NEXT, Type.longType());
        return out;
    }

    /** Build a physical Record stamping (xmin, xmax=ALIVE, next=NO_NEXT). */
    public static Record attachVersion(Record userRecord, long xmin) {
        return attachVersion(userRecord, xmin, ALIVE, NO_NEXT);
    }

    /** Convenience for back-compat call sites that stamp xmin and xmax only. */
    public static Record attachVersion(Record userRecord, long xmin, long xmax) {
        return attachVersion(userRecord, xmin, xmax, NO_NEXT);
    }

    public static Record attachVersion(Record userRecord, long xmin, long xmax, long next) {
        List<DataBox> values = new ArrayList<>(userRecord.getValues());
        values.add(new LongDataBox(xmin));
        values.add(new LongDataBox(xmax));
        values.add(new LongDataBox(next));
        return new Record(values);
    }

    /** Strip the trailing stamps from a physical Record to recover the user view. */
    public static Record userRecord(Record physical) {
        List<DataBox> all = physical.getValues();
        return new Record(all.subList(0, all.size() - 3));
    }

    public static long xmin(Record physical) {
        return physical.getValue(physical.size() - 3).getLong();
    }

    public static long xmax(Record physical) {
        return physical.getValue(physical.size() - 2).getLong();
    }

    public static long nextEncoded(Record physical) {
        return physical.getValue(physical.size() - 1).getLong();
    }

    /** Decoded next RecordId, or null if this is the tip of the chain. */
    public static RecordId next(Record physical) {
        return decodeRid(nextEncoded(physical));
    }

    public static Record withXmax(Record physical, long newXmax) {
        List<DataBox> values = new ArrayList<>(physical.getValues());
        values.set(values.size() - 2, new LongDataBox(newXmax));
        return new Record(values);
    }

    public static Record withNext(Record physical, RecordId nextRid) {
        List<DataBox> values = new ArrayList<>(physical.getValues());
        values.set(values.size() - 1, new LongDataBox(encodeRid(nextRid)));
        return new Record(values);
    }

    /** Set xmax and next_rid together — how an UPDATE stamps the prior version. */
    public static Record withXmaxAndNext(Record physical, long newXmax, RecordId nextRid) {
        List<DataBox> values = new ArrayList<>(physical.getValues());
        values.set(values.size() - 2, new LongDataBox(newXmax));
        values.set(values.size() - 1, new LongDataBox(encodeRid(nextRid)));
        return new Record(values);
    }

    // --- RecordId <-> long codec ---

    public static long encodeRid(RecordId rid) {
        if (rid == null) return NO_NEXT;
        return (rid.getPageNum() << 16) | (rid.getEntryNum() & 0xFFFFL);
    }

    public static RecordId decodeRid(long encoded) {
        if (encoded == NO_NEXT) return null;
        long pageNum = encoded >>> 16;
        short entryNum = (short) (encoded & 0xFFFFL);
        return new RecordId(pageNum, entryNum);
    }
}
