package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.categories.ProjTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.databox.LongDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.*;

/**
 * MvccSchema wraps a user-facing Schema with two trailing system columns
 * (__xmin, __xmax) so each persisted Record carries its version metadata.
 *
 * The wrapping is transparent to callers that work with user records:
 *   - attachVersion(userRecord, xmin, xmax) -> physicalRecord
 *   - userRecord(physicalRecord) -> userRecord
 *   - xmin(physicalRecord) / xmax(physicalRecord) -> version stamps
 */
@Category({ProjTests.class, PublicTests.class})
public class TestMvccSchema {

    private Schema userSchema() {
        return new Schema().add("id", Type.intType()).add("name", Type.stringType(8));
    }

    private Record phys(Object id, Object name, long xmin, long xmax, long next) {
        return new Record(id, name, new LongDataBox(xmin), new LongDataBox(xmax),
                          new LongDataBox(next));
    }

    @Test
    public void physicalSchemaAppendsXminXmaxNext() {
        Schema phys = MvccSchema.physical(userSchema());
        List<String> names = phys.getFieldNames();
        assertEquals(5, names.size());
        assertEquals("id", names.get(0));
        assertEquals("name", names.get(1));
        assertEquals(MvccSchema.XMIN, names.get(2));
        assertEquals(MvccSchema.XMAX, names.get(3));
        assertEquals(MvccSchema.NEXT, names.get(4));
        assertEquals(Type.longType(), phys.getFieldType(2));
        assertEquals(Type.longType(), phys.getFieldType(3));
        assertEquals(Type.longType(), phys.getFieldType(4));
    }

    @Test
    public void attachVersionProducesRecordWithStamps() {
        Record user = new Record(42, "alice");
        Record p = MvccSchema.attachVersion(user, 7L, 0L);
        assertEquals(5, p.size());
        assertEquals(42, p.getValue(0).getInt());
        assertEquals("alice", p.getValue(1).getString().trim());
        assertEquals(7L, MvccSchema.xmin(p));
        assertEquals(0L, MvccSchema.xmax(p));
        assertEquals(MvccSchema.NO_NEXT, MvccSchema.nextEncoded(p));
    }

    @Test
    public void userRecordStripsTrailingStamps() {
        Record p = phys(42, "alice", 7L, 0L, MvccSchema.NO_NEXT);
        Record user = MvccSchema.userRecord(p);
        assertEquals(2, user.size());
        assertEquals(42, user.getValue(0).getInt());
    }

    @Test
    public void xminAndXmaxExtractFromPhysical() {
        Record p = phys(1, "x", 42L, 99L, MvccSchema.NO_NEXT);
        assertEquals(42L, MvccSchema.xmin(p));
        assertEquals(99L, MvccSchema.xmax(p));
    }

    @Test
    public void withXmaxReturnsUpdatedPhysicalRecord() {
        Record p = phys(1, "x", 42L, 0L, MvccSchema.NO_NEXT);
        Record updated = MvccSchema.withXmax(p, 55L);
        assertEquals(0L, MvccSchema.xmax(p));       // original immutable
        assertEquals(55L, MvccSchema.xmax(updated));
        assertEquals(1, updated.getValue(0).getInt());
        assertEquals(42L, MvccSchema.xmin(updated));
    }

    @Test
    public void withNextReturnsUpdatedPhysicalRecord() {
        Record p = phys(1, "x", 42L, 0L, MvccSchema.NO_NEXT);
        RecordId nextRid = new RecordId(5L, (short) 7);
        Record updated = MvccSchema.withNext(p, nextRid);
        assertEquals(MvccSchema.NO_NEXT, MvccSchema.nextEncoded(p));
        assertEquals(nextRid, MvccSchema.next(updated));
        assertEquals(42L, MvccSchema.xmin(updated));
        assertEquals(0L, MvccSchema.xmax(updated));
    }

    @Test
    public void ridEncodeDecodeRoundTrip() {
        RecordId rid = new RecordId(12345L, (short) 678);
        long encoded = MvccSchema.encodeRid(rid);
        assertEquals(rid, MvccSchema.decodeRid(encoded));
    }

    @Test
    public void decodeRidReturnsNullForNoNext() {
        assertNull(MvccSchema.decodeRid(MvccSchema.NO_NEXT));
    }

    @Test
    public void roundTripUserToPhysicalToUser() {
        Record original = new Record(7, "bob");
        Record p = MvccSchema.attachVersion(original, 100L, 0L);
        Record back = MvccSchema.userRecord(p);
        assertEquals(original, back);
    }
}
