package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;

/** @author jonathan */
public class MiruFieldFilter {

    public final String fieldName;
    public final List<String> values;

    public MiruFieldFilter(
        String fieldName,
        List<String> values) {
        this.fieldName = fieldName;
        this.values = values;
    }

    @JsonCreator
    public static MiruFieldFilter fromJson(
        @JsonProperty("fieldName") String fieldName,
        @JsonProperty("values") List<String> values) {
        return new MiruFieldFilter(fieldName, ImmutableList.copyOf(values));
    }

    @Override
    public String toString() {
        return "FieldFilter{" + "fieldName=" + fieldName + ", values=" + values + '}';
    }

}
