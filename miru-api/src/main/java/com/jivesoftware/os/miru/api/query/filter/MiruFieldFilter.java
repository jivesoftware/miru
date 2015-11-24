package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import java.io.Serializable;
import java.util.List;

/** @author jonathan */
public class MiruFieldFilter implements Serializable {

    public final MiruFieldType fieldType;
    public final String fieldName;
    public final List<String> values;
    public final List<MiruTermId> rawValues;

    @JsonCreator
    public MiruFieldFilter(@JsonProperty("fieldType") MiruFieldType fieldType,
        @JsonProperty("fieldName") String fieldName,
        @JsonProperty("values") List<String> values) {
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.values = values;
        this.rawValues = null;
    }

    private MiruFieldFilter(MiruFieldType fieldType, String fieldName, List<String> values, List<MiruTermId> rawValues) {
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.values = values;
        this.rawValues = rawValues;
    }

    public static MiruFieldFilter raw(MiruFieldType fieldType, String fieldName, List<MiruTermId> rawValues) {
        return new MiruFieldFilter(fieldType, fieldName, null, rawValues);
    }

    @Override
    public String toString() {
        return "MiruFieldFilter{" +
            "fieldType=" + fieldType +
            ", fieldName='" + fieldName + '\'' +
            ", values=" + values +
            ", rawValues=" + rawValues +
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

        MiruFieldFilter that = (MiruFieldFilter) o;

        if (fieldType != that.fieldType) {
            return false;
        }
        if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
            return false;
        }
        if (values != null ? !values.equals(that.values) : that.values != null) {
            return false;
        }
        return !(rawValues != null ? !rawValues.equals(that.rawValues) : that.rawValues != null);

    }

    @Override
    public int hashCode() {
        int result = fieldType != null ? fieldType.hashCode() : 0;
        result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
        result = 31 * result + (values != null ? values.hashCode() : 0);
        result = 31 * result + (rawValues != null ? rawValues.hashCode() : 0);
        return result;
    }
}
