package com.jivesoftware.os.miru.service.index.disk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.SwappingFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.service.activity.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Persistent impl. Activity data is mem-mapped. Does not support set().
 * The last index is stored on disk and cached via AtomicInteger.
 */
public class MiruMemMappedActivityIndex implements MiruActivityIndex, BulkImport<MiruInternalActivity[]> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruFilerProvider filerProvider;
    private final FileBackedKeyedStore keyedStore;
    private final ObjectMapper objectMapper;
    private final AtomicInteger indexSize;

    private Filer filer;

    public MiruMemMappedActivityIndex(MiruFilerProvider filerProvider, File mapDirectory, File swapDirectory, ChunkStore chunkStore, ObjectMapper objectMapper)
        throws Exception {

        this.filerProvider = filerProvider;
        this.keyedStore = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 4, 100, chunkStore, 512);
        this.objectMapper = objectMapper;
        this.indexSize = new AtomicInteger(0);

        File file = filerProvider.getBackingFile();
        if (file.length() > 0) {
            filer = filerProvider.getFiler(file.length());
        }
    }

    @Override
    public MiruInternalActivity get(int index) {
        checkArgument(index >= 0 && index < capacity(), "Index parameter is out of bounds. The value " + index + " must be >=0 and <" + capacity());
        try {
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), false);
            if (swappableFiler != null) {
                byte[] bytes;
                synchronized (swappableFiler.lock()) {
                    swappableFiler.sync();
                    swappableFiler.seek(0);
                    bytes = FilerIO.readByteArray(swappableFiler, "activity");
                }
                return objectMapper.readValue(bytes, MiruInternalActivity.class);
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int lastId() {
        return capacity() - 1;
    }

    @Override
    public void set(int index, MiruInternalActivity activity) {
        checkArgument(index >= 0, "Index parameter is out of bounds. The value " + index + " must be >=0");
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(activity);
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), true);
            synchronized (swappableFiler.lock()) {
                SwappingFiler swappingFiler = swappableFiler.swap(4 + bytes.length);
                FilerIO.writeByteArray(swappingFiler, bytes, "activity");
                swappingFiler.commit();
            }
            checkCapacity(index);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return filer.length() + keyedStore.mapStoreSizeInBytes();
    }

    private void checkCapacity(int index) throws IOException {
        int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                synchronized (filer.lock()) {
                    filer.seek(0);
                    FilerIO.writeInt(filer, size, "size");
                }
                indexSize.set(size);
            }
        }
    }

    private int capacity() {
        try {
            int size = indexSize.get();
            if (size == 0) {
                synchronized (filer.lock()) {
                    filer.seek(0);
                    size = FilerIO.readInt(filer, "size");
                }
                indexSize.set(size);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (filer != null) {
                filer.close();
            }
        } catch (IOException e) {
            log.error("Failed to remove activity index", e);
        }
    }

    @Override
    public void bulkImport(BulkExport<MiruInternalActivity[]> bulkExport) throws Exception {
        MiruInternalActivity[] importActivities = bulkExport.bulkExport();

        int lastIndex;
        for (lastIndex = importActivities.length - 1; lastIndex >= 0 && importActivities[lastIndex] == null; lastIndex--) {
            // walk to first non-null
        }
        int indexSize = lastIndex + 1;

        long length;
        try (RandomAccessFiler filer = new RandomAccessFiler(filerProvider.getBackingFile(), "rw")) {
            synchronized (filer.lock()) {
                filer.seek(0);
                FilerIO.writeInt(filer, indexSize, "size");
                length = filer.length();
            }
        }

        this.filer = filerProvider.getFiler(length);

        for (int index = 0; index <= lastIndex; index++) {
            MiruInternalActivity activity = importActivities[index];
            if (activity != null) {
                set(index, activity);
            }
        }
    }
}
