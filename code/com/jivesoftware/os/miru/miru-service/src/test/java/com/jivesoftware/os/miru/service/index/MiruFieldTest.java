package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskField;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryField;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryIndex;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruFieldTest {

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public void getInvertedIndex(MiruField field, List<Integer> ids) throws Exception {
        for (int id : ids) {
            Optional<MiruInvertedIndex> optional = field.getInvertedIndex(new MiruTermId(new byte[] { (byte) id }));
            assertTrue(optional.isPresent());
            MiruInvertedIndex invertedIndex = optional.get();
            assertEquals(invertedIndex.getIndex().cardinality(), 1);
            assertTrue(invertedIndex.getIndex().get(id));
        }
    }

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public void getInvertedIndexWithConsideration(MiruField field, List<Integer> ids) throws Exception {
        // this works because maxId = id in our termToIndex maps
        int median = ids.get(ids.size() / 2);

        for (int id : ids) {
            Optional<MiruInvertedIndex> optional = field.getInvertedIndex(new MiruTermId(new byte[] { (byte) id }), median);
            assertEquals(optional.isPresent(), id > median);
        }
    }

    @DataProvider(name = "miruFieldDataProvider")
    public Object[][] miruFieldDataProvider() throws Exception {
        List<Integer> ids = Lists.newArrayList();

        MiruInMemoryField miruInMemoryField = new MiruInMemoryField(0, Maps.<MiruTermId, MiruFieldIndexKey>newHashMap(), new MiruInMemoryIndex());
        for (int id = 0; id < 10; id++) {
            ids.add(id);
            miruInMemoryField.index(new MiruTermId(new byte[] { (byte) id }), id);
        }

        File indexMapDirectory = Files.createTempDirectory(getClass().getSimpleName()).toFile();
        File indexSwapDirectory = Files.createTempDirectory(getClass().getSimpleName()).toFile();
        File chunkDirectory = Files.createTempDirectory(getClass().getSimpleName()).toFile();
        File chunkFile = new File(chunkDirectory, "chunk");
        File fieldMapDirectory = Files.createTempDirectory(getClass().getSimpleName()).toFile();

        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunkFile.getAbsolutePath(), 5120, false); // 512 min size, times 10 field indexes
        MiruOnDiskField miruOnDiskField = new MiruOnDiskField(0, new MiruOnDiskIndex(indexMapDirectory, indexSwapDirectory, chunkStore), fieldMapDirectory);
        // need to export/import both the field and its index (a little strange)
        miruOnDiskField.bulkImport(miruInMemoryField);
        miruOnDiskField.getIndex().bulkImport(miruInMemoryField.getIndex());

        return new Object[][] {
            { miruInMemoryField, ids },
            { miruOnDiskField, ids }
        };
    }
}
