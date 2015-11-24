package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;

/** @author jonathan */
public class MiruFilter implements Serializable {

    public static final MiruFilter NO_FILTER = new MiruFilter(
        MiruFilterOperation.or,
        false,
        null,
        null);

    public final MiruFilterOperation operation;
    public final boolean inclusiveFilter;
    public final List<MiruFieldFilter> fieldFilters;
    public final List<MiruFilter> subFilters;

    private int hash = 0;

    @JsonCreator
    public MiruFilter(@JsonProperty("operation") MiruFilterOperation operation,
        @JsonProperty("inclusiveFilter") boolean inclusiveFilter,
        @JsonProperty("fieldFilters") List<MiruFieldFilter> fieldFilters,
        @JsonProperty("subFilters") List<MiruFilter> subFilters) {
        this.operation = operation;
        this.inclusiveFilter = inclusiveFilter;
        this.fieldFilters = fieldFilters;
        this.subFilters = subFilters;
    }

    @Override
    public String toString() {
        return "MiruFilter{" +
            "operation=" + operation +
            ", inclusiveFilter=" + inclusiveFilter +
            ", fieldFilters=" + fieldFilters +
            ", subFilters=" + subFilters +
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

        MiruFilter that = (MiruFilter) o;

        if (inclusiveFilter != that.inclusiveFilter) {
            return false;
        }
        if (fieldFilters != null ? !fieldFilters.equals(that.fieldFilters) : that.fieldFilters != null) {
            return false;
        }
        if (operation != that.operation) {
            return false;
        }
        if (subFilters != null ? !subFilters.equals(that.subFilters) : that.subFilters != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int result = operation != null ? operation.hashCode() : 0;
            result = 31 * result + (inclusiveFilter ? 1 : 0);
            result = 31 * result + (fieldFilters != null ? fieldFilters.hashCode() : 0);
            result = 31 * result + (subFilters != null ? subFilters.hashCode() : 0);
            hash = result;
        }
        return hash;
    }
}
