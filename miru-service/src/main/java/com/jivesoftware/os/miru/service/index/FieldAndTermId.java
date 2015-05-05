package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public class FieldAndTermId {

    public final int fieldId;
    public final byte[] termId;

    public FieldAndTermId(int fieldId, byte[] termId) {
        this.fieldId = fieldId;
        this.termId = termId;
    }
}
