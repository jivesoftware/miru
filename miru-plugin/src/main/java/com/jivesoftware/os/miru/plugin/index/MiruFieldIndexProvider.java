package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.field.MiruFieldType;

/**
 *
 */
public class MiruFieldIndexProvider<IBM> {

    private final MiruFieldIndex<IBM>[] indexes;

    public MiruFieldIndexProvider(MiruFieldIndex<IBM>[] indexes) {
        Preconditions.checkArgument(indexes.length == MiruFieldType.values().length);
        this.indexes = indexes;
    }

    public MiruFieldIndex<IBM> getFieldIndex(MiruFieldType type) {
        return indexes[type.getIndex()];
    }
}
