package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskInboxIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInboxIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInboxIndex.InboxAndLastActivityIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInvertedIndex;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruInboxIndexTest {

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxWithoutCreating(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {

        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(miruStreamId);
        assertNotNull(inbox);
        assertFalse(inbox.isPresent());
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxAndCreate(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {

        MiruInvertedIndexAppender inbox = miruInboxIndex.getAppender(miruStreamId);
        assertNotNull(inbox);
        Optional<EWAHCompressedBitmap> inboxIndex = miruInboxIndex.getInbox(miruStreamId);
        assertNotNull(inboxIndex);
        assertTrue(inboxIndex.isPresent());
        assertEquals(inboxIndex.get().sizeInBytes(), 8);
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testIndexIds(MiruInboxIndex miruInboxIndex, MiruStreamId miruStreamId) throws Exception {
        miruInboxIndex.index(miruStreamId, 1);
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testSetLastActivityIndex(MiruInboxIndex miruInboxIndex, MiruStreamId miruStreamId) throws Exception {
        miruInboxIndex.setLastActivityIndex(miruStreamId, 1);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testIndexedData(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, List<Integer> indexedIds) throws Exception {
        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(streamId);
        assertNotNull(inbox);
        assertTrue(inbox.isPresent());
        assertTrue(inbox.get().get(indexedIds.get(0)));
        assertTrue(inbox.get().get(indexedIds.get(1)));
        assertTrue(inbox.get().get(indexedIds.get(2)));
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testLastActivityIndexNotSetAutomatically(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, List<Integer> indexedIds)
        throws Exception {

        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(streamId);
        assertEquals(lastActivityIndex, -1);

        int nextId = Integer.MAX_VALUE / 2;
        miruInboxIndex.index(streamId, nextId);
        lastActivityIndex = miruInboxIndex.getLastActivityIndex(streamId);
        assertEquals(lastActivityIndex, -1);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testDefaultLastActivityIndexWithNewStreamId(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId,
        List<Integer> indexedIds) throws Exception {

        MiruStreamId newStreamId = new MiruStreamId(new Id(1_337).toBytes());
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId);
        assertEquals(lastActivityIndex, -1);

        int nextId = Integer.MAX_VALUE / 2;
        miruInboxIndex.index(newStreamId, nextId);
        lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId);
        assertEquals(lastActivityIndex, -1);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testLastActivityIndex(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, List<Integer> indexedIds) throws Exception {

        int nextId = Integer.MAX_VALUE / 3;
        miruInboxIndex.setLastActivityIndex(streamId, nextId);
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(streamId);
        assertEquals(lastActivityIndex, nextId);
    }

    @DataProvider(name = "miruInboxIndexDataProvider")
    public Object[][] miruInboxIndexDataProvider() throws Exception {
        MiruStreamId miruStreamId = new MiruStreamId(new Id(12_345).toBytes());
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(10);
        MiruInMemoryInboxIndex<EWAHCompressedBitmap> miruInMemoryInboxIndex = new MiruInMemoryInboxIndex<>(bitmaps);

        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swap").toFile().getAbsolutePath(),
            Files.createTempDirectory("swap").toFile().getAbsolutePath()
        };
        String chunksDir = Files.createTempDirectory("chunk").toFile().getAbsolutePath();
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunksDir, "chunk", 4_096, false);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);
        MiruOnDiskInboxIndex<EWAHCompressedBitmap> miruOnDiskInboxIndex = new MiruOnDiskInboxIndex<>(bitmaps, mapDirs, swapDirs, multiChunkStore);
        miruOnDiskInboxIndex.bulkImport(tenantId, miruInMemoryInboxIndex);

        return new Object[][] {
            { miruInMemoryInboxIndex, miruStreamId },
            { miruOnDiskInboxIndex, miruStreamId }
        };
    }

    @DataProvider(name = "miruInboxIndexDataProviderWithData")
    public Object[][] miruInboxIndexDataProviderWithData() throws Exception {
        MiruStreamId streamId = new MiruStreamId(new Id(12_345).toBytes());

        // Create in-memory inbox index
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(10);
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruInMemoryInboxIndex<EWAHCompressedBitmap> miruInMemoryInboxIndex = new MiruInMemoryInboxIndex<>(bitmaps);

        ConcurrentMap<MiruStreamId, MiruInvertedIndex<EWAHCompressedBitmap>> index = new ConcurrentHashMap<>();
        Map<MiruStreamId, Integer> lastActivityIndex = Maps.newHashMap();

        // Add activities to index
        EWAHCompressedBitmap invertedIndex = new EWAHCompressedBitmap();
        invertedIndex.set(1);
        invertedIndex.set(2);
        invertedIndex.set(3);
        MiruInMemoryInvertedIndex<EWAHCompressedBitmap> ii = new MiruInMemoryInvertedIndex<>(bitmaps);
        ii.or(invertedIndex);
        index.put(streamId, ii);

        final InboxAndLastActivityIndex<EWAHCompressedBitmap> inboxAndLastActivityIndex = new InboxAndLastActivityIndex<>(index, lastActivityIndex);
        miruInMemoryInboxIndex.bulkImport(tenantId, new BulkExport<InboxAndLastActivityIndex<EWAHCompressedBitmap>>() {
            @Override
            public InboxAndLastActivityIndex<EWAHCompressedBitmap> bulkExport(MiruTenantId tenantId) throws Exception {
                return inboxAndLastActivityIndex;
            }
        });

        // Copy to on disk index
        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swap").toFile().getAbsolutePath(),
            Files.createTempDirectory("swap").toFile().getAbsolutePath()
        };
        String chunksDir = Files.createTempDirectory("chunk").toFile().getAbsolutePath();
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunksDir, "chunk", 4_096, false);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);
        MiruOnDiskInboxIndex<EWAHCompressedBitmap> miruOnDiskInboxIndex = new MiruOnDiskInboxIndex<>(bitmaps, mapDirs, swapDirs, multiChunkStore);
        miruOnDiskInboxIndex.bulkImport(tenantId, miruInMemoryInboxIndex);

        return new Object[][] {
            { miruInMemoryInboxIndex, streamId, ImmutableList.of(1, 2, 3) },
            { miruOnDiskInboxIndex, streamId, ImmutableList.of(1, 2, 3) }
        };
    }
}
