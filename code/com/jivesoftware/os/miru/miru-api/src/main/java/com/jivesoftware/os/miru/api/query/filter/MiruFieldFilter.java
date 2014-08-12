package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.List;

/** @author jonathan */
public class MiruFieldFilter {

    public final String fieldName;
    public final ImmutableList<MiruTermId> values;

    public MiruFieldFilter(
        String fieldName,
        ImmutableList<MiruTermId> values) {
        this.fieldName = fieldName;
        this.values = values;
    }

    @JsonCreator
    public static MiruFieldFilter fromJson(
        @JsonProperty("fieldName") String fieldName,
        @JsonProperty("values") List<MiruTermId> values) {
        return new MiruFieldFilter(fieldName, ImmutableList.copyOf(values));
    }

    @Override
    public String toString() {
        return "FieldFilter{" + "fieldName=" + fieldName + ", values=" + values + '}';
    }

}
