package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.field.MiruFieldType;

/**
 *
 */
public class MiruFieldIndexProvider<BM> {

    private final MiruFieldIndex<BM>[] indexes;

    public MiruFieldIndexProvider(MiruFieldIndex<BM>[] indexes) {
        Preconditions.checkArgument(indexes.length == MiruFieldType.values().length);
        this.indexes = indexes;
    }

    public MiruFieldIndex<BM> getFieldIndex(MiruFieldType type) {
        return indexes[type.getIndex()];
    }
}
