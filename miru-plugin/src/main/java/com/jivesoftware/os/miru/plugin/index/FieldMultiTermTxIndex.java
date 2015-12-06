package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public class FieldMultiTermTxIndex<BM extends IBM, IBM> implements MiruMultiTxIndex<IBM> {

    private final MiruFieldIndex<BM, IBM> fieldIndex;
    private final int fieldId;
    private MiruTermId[] termIds;

    public FieldMultiTermTxIndex(MiruFieldIndex<BM, IBM> fieldIndex, int fieldId) {
        this.fieldIndex = fieldIndex;
        this.fieldId = fieldId;
    }

    public void setTermIds(MiruTermId[] termIds) {
        this.termIds = termIds;
    }

    @Override
    public void txIndex(MultiIndexTx<IBM> tx, StackBuffer stackBuffer) throws Exception {
        if (termIds == null) {
            throw new IllegalStateException("Terms need to be set before invoking txIndex");
        }
        fieldIndex.multiTxIndex(fieldId, termIds, stackBuffer, tx);
    }
}
