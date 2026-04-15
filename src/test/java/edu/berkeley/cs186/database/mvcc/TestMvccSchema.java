package edu.berkeley.cs186.database.mvcc;

import edu.berkeley.cs186.database.categories.ProjTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.databox.LongDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.Record;
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

    @Test
    public void physicalSchemaAppendsXminXmax() {
        Schema phys = MvccSchema.physical(userSchema());
        List<String> names = phys.getFieldNames();
        assertEquals(4, names.size());
        assertEquals("id", names.get(0));
        assertEquals("name", names.get(1));
        assertEquals(MvccSchema.XMIN, names.get(2));
        assertEquals(MvccSchema.XMAX, names.get(3));
        assertEquals(Type.longType(), phys.getFieldType(2));
        assertEquals(Type.longType(), phys.getFieldType(3));
    }

    @Test
    public void attachVersionProducesRecordWithStamps() {
        Record user = new Record(42, "alice");
        Record phys = MvccSchema.attachVersion(user, 7L, 0L);
        assertEquals(4, phys.size());
        assertEquals(42, phys.getValue(0).getInt());
        assertEquals("alice", phys.getValue(1).getString().trim());
        assertEquals(7L, phys.getValue(2).getLong());
        assertEquals(0L, phys.getValue(3).getLong());
    }

    @Test
    public void userRecordStripsTrailingStamps() {
        Record phys = new Record(42, "alice", new LongDataBox(7L), new LongDataBox(0L));
        Record user = MvccSchema.userRecord(phys);
        assertEquals(2, user.size());
        assertEquals(42, user.getValue(0).getInt());
    }

    @Test
    public void xminAndXmaxExtractFromPhysical() {
        Record phys = new Record(1, "x", new LongDataBox(42L), new LongDataBox(99L));
        assertEquals(42L, MvccSchema.xmin(phys));
        assertEquals(99L, MvccSchema.xmax(phys));
    }

    @Test
    public void withXmaxReturnsUpdatedPhysicalRecord() {
        Record phys = new Record(1, "x", new LongDataBox(42L), new LongDataBox(0L));
        Record updated = MvccSchema.withXmax(phys, 55L);
        // Original untouched (records are value-like)
        assertEquals(0L, MvccSchema.xmax(phys));
        assertEquals(55L, MvccSchema.xmax(updated));
        // User columns preserved
        assertEquals(1, updated.getValue(0).getInt());
        assertEquals(42L, MvccSchema.xmin(updated));
    }

    @Test
    public void roundTripUserToPhysicalToUser() {
        Record original = new Record(7, "bob");
        Record phys = MvccSchema.attachVersion(original, 100L, 0L);
        Record back = MvccSchema.userRecord(phys);
        assertEquals(original, back);
    }
}
