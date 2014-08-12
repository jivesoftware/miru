package com.jivesoftware.os.miru.service.index;

/** @author jonathan */
public class MiruFields {

    private final MiruField[] fields;
    private final MiruIndex index;

    public MiruFields(MiruField[] fields, MiruIndex index) {
        this.fields = fields;
        this.index = index;
    }

    public MiruField getField(int fieldId) {
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
