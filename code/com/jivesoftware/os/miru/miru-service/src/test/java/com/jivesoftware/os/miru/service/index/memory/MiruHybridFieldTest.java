package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.DirectByteBufferFactory;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import java.nio.file.Files;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruHybridFieldTest {

    @Test
    public void testMigrate() throws Exception {
        DirectByteBufferFactory directByteBufferFactory = new DirectByteBufferFactory();
        MiruHybridField<RoaringBitmap> hybridField = new MiruHybridField<>(
            new MiruFieldDefinition(0, "doc"),
            new MiruInMemoryIndex<>(new MiruBitmapsRoaring(), directByteBufferFactory),
            directByteBufferFactory,
            new String[] {
                Files.createTempDirectory("field").toFile().getAbsolutePath(),
                Files.createTempDirectory("field").toFile().getAbsolutePath()
            },
            16);

        final int numTerms = 100;
        for (int i = 0; i < numTerms; i++) {
            hybridField.index(new MiruTermId(new byte[] { (byte) i }), i);
        }

        for (int i = 0; i < numTerms; i++) {
            Optional<MiruInvertedIndex<RoaringBitmap>> invertedIndex = hybridField.getInvertedIndex(new MiruTermId(new byte[] { (byte) i }));
            assertTrue(invertedIndex.isPresent());
            assertFalse(invertedIndex.get().getIndex().contains(i - 1));
            assertTrue(invertedIndex.get().getIndex().contains(i));
            assertFalse(invertedIndex.get().getIndex().contains(i + 1));
        }

        hybridField.notifyStateChange(MiruPartitionState.online);

        for (int i = 0; i < numTerms; i++) {
            Optional<MiruInvertedIndex<RoaringBitmap>> invertedIndex = hybridField.getInvertedIndex(new MiruTermId(new byte[] { (byte) i }));
            assertTrue(invertedIndex.isPresent());
            assertFalse(invertedIndex.get().getIndex().contains(i - 1));
            assertTrue(invertedIndex.get().getIndex().contains(i));
            assertFalse(invertedIndex.get().getIndex().contains(i + 1));
        }
    }

}