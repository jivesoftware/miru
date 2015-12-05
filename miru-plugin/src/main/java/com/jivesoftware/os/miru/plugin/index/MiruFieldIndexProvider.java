package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.field.MiruFieldType;

/**
 *
 */
public class MiruFieldIndexProvider<BM extends IBM, IBM> {

    private final MiruFieldIndex<BM, IBM>[] indexes;

    public MiruFieldIndexProvider(MiruFieldIndex<BM, IBM>[] indexes) {
        Preconditions.checkArgument(indexes.length == MiruFieldType.values().length);
        this.indexes = indexes;
    }

    public MiruFieldIndex<BM, IBM> getFieldIndex(MiruFieldType type) {
        return indexes[type.getIndex()];
    }
}
