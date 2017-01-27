package com.jivesoftware.os.miru.service.index.lab;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex.CustomMarshaller;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class LabSipIndexTest {

    @Test
    public void testCustom() throws Exception {
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(1), new MiruHost("logicalName"));
        MiruSipIndex<RCVSSipCursor> sipIndex = buildOnDiskContext(4, true, true, bitmaps, coord).sipIndex;

        for (int i = 0; i < 10; i++) {
            Integer got = sipIndex.getCustom(FilerIO.intBytes(i), MARSHALLER);
            assertNull(got);
        }

        for (int i = 0; i < 10; i++) {
            boolean result = sipIndex.setCustom(FilerIO.intBytes(i), i, MARSHALLER);
            assertTrue(result);
        }

        for (int i = 0; i < 10; i++) {
            Integer got = sipIndex.getCustom(FilerIO.intBytes(i), MARSHALLER);
            assertNotNull(got);
            assertEquals(got.intValue(), i);
        }

        sipIndex.merge();

        for (int i = 0; i < 10; i++) {
            Integer got = sipIndex.getCustom(FilerIO.intBytes(i), MARSHALLER);
            assertNotNull(got);
            assertEquals(got.intValue(), i);
        }
    }

    private static final CustomMarshaller<Integer> MARSHALLER = new CustomMarshaller<Integer>() {
        @Override
        public byte[] toBytes(Integer comparable) {
            return FilerIO.intBytes(comparable);
        }

        @Override
        public Integer fromBytes(byte[] bytes) {
            return FilerIO.bytesInt(bytes);
        }
    };
}