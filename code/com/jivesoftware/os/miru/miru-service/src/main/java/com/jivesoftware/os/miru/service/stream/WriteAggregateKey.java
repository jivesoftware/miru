package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
class WriteAggregateKey {
    private final MiruTermId fieldValue;
    private final MiruTermId aggregateFieldValue;

    WriteAggregateKey(MiruTermId fieldValue, MiruTermId aggregateFieldValue) {
        this.fieldValue = fieldValue;
        this.aggregateFieldValue = aggregateFieldValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WriteAggregateKey that = (WriteAggregateKey) o;

        if (aggregateFieldValue != null ? !aggregateFieldValue.equals(that.aggregateFieldValue) : that.aggregateFieldValue != null) {
            return false;
        }
        return !(fieldValue != null ? !fieldValue.equals(that.fieldValue) : that.fieldValue != null);
    }

    @Override
    public int hashCode() {
        int result = fieldValue != null ? fieldValue.hashCode() : 0;
        result = 31 * result + (aggregateFieldValue != null ? aggregateFieldValue.hashCode() : 0);
        return result;
    }
}
