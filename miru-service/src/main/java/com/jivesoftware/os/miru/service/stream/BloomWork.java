package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.List;

/**
 *
 */
class BloomWork implements Comparable<BloomWork> {
    final int fieldId;
    final int bloomFieldId;
    final MiruTermId fieldValue;
    final List<MiruTermId> bloomFieldValues;

    BloomWork(int fieldId, int bloomFieldId, MiruTermId fieldValue, List<MiruTermId> bloomFieldValues) {
        this.fieldId = fieldId;
        this.bloomFieldId = bloomFieldId;
        this.fieldValue = fieldValue;
        this.bloomFieldValues = bloomFieldValues;
    }

    @Override
    public int compareTo(BloomWork o) {
        // flipped so that natural ordering is "largest first"
        return Integer.compare(o.bloomFieldValues.size(), bloomFieldValues.size());
    }
}
