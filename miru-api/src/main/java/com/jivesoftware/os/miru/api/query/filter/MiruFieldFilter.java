package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** @author jonathan */
public class MiruFieldFilter implements Serializable {

    public final MiruFieldType fieldType;
    public final String fieldName;
    public final List<MiruValue> values;

    @JsonCreator
    public MiruFieldFilter(@JsonProperty("fieldType") MiruFieldType fieldType,
        @JsonProperty("fieldName") String fieldName,
        @JsonProperty("values") List<MiruValue> values) {
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.values = values;
    }

    public static MiruFieldFilter of(MiruFieldType fieldType, String fieldName, Object... values) {
        return of(fieldType, fieldName, Arrays.asList(values));
    }

    public static MiruFieldFilter of(MiruFieldType fieldType, String fieldName, Collection<Object> values) {
        List<MiruValue> miruValues = Lists.newArrayListWithCapacity(values.size());
        for (Object value : values) {
            miruValues.add(new MiruValue(value.toString()));
        }
        return new MiruFieldFilter(fieldType, fieldName, miruValues);
    }

    public static MiruFieldFilter of(MiruFieldType fieldType, String fieldName, MiruValue... values) {
        return new MiruFieldFilter(fieldType, fieldName, Arrays.asList(values));
    }

    @Override
    public String toString() {
        return "MiruFieldFilter{" +
            "fieldType=" + fieldType +
            ", fieldName='" + fieldName + '\'' +
            ", values=" + values +
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
        return !(values != null ? !values.equals(that.values) : that.values != null);

    }

    @Override
    public int hashCode() {
        int result = fieldType != null ? fieldType.hashCode() : 0;
        result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
        result = 31 * result + (values != null ? values.hashCode() : 0);
        return result;
    }
}
