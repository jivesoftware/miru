package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public class FieldAndTermId {

    public final int fieldId;
    public final MiruTermId termId;

    public FieldAndTermId(int fieldId, MiruTermId termId) {
        this.fieldId = fieldId;
        this.termId = termId;
    }

    @Override
    public String toString() {
        return "FieldAndTermId{" +
            "fieldId=" + fieldId +
            ", termId=" + termId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldAndTermId that = (FieldAndTermId) o;

        if (fieldId != that.fieldId) {
            return false;
        }
        return !(termId != null ? !termId.equals(that.termId) : that.termId != null);

    }

    @Override
    public int hashCode() {
        int result = fieldId;
        result = 31 * result + (termId != null ? termId.hashCode() : 0);
        return result;
    }
}
