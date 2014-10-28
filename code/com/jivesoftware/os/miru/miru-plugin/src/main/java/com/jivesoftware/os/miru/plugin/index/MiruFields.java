package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.MiruPartitionState;

/** @author jonathan */
public class MiruFields<BM> {

    private final MiruField<BM>[] fields;
    private final MiruIndex index;

    public MiruFields(MiruField<BM>[] fields, MiruIndex index) {
        this.fields = fields;
        this.index = index;
    }

    public void notifyStateChange(MiruPartitionState state) throws Exception {
        for (MiruField<BM> field : fields) {
            field.notifyStateChange(state);
        }
    }

    public MiruField<BM> getField(int fieldId) {
        return fields[fieldId];
    }

    public long sizeInMemory() throws Exception {
        long sizeInBytes = index.sizeInMemory();
        for (MiruField field : fields) {
            sizeInBytes += field.sizeInMemory();
        }
        return sizeInBytes;
    }

    public long sizeOnDisk() throws Exception {
        long sizeInBytes = index.sizeOnDisk();
        for (MiruField field : fields) {
            sizeInBytes += field.sizeOnDisk();
        }
        return sizeInBytes;
    }

}
