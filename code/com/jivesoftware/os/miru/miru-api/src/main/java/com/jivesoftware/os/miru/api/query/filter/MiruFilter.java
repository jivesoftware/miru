package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import java.util.List;

/** @author jonathan */
public class MiruFilter {

    public static final MiruFilter NO_FILTER = new MiruFilter(
        MiruFilterOperation.or,
        Optional.<ImmutableList<MiruFieldFilter>>absent(),
        Optional.<ImmutableList<MiruFilter>>absent());

    public final MiruFilterOperation operation;
    public final Optional<ImmutableList<MiruFieldFilter>> fieldFilters;
    public final Optional<ImmutableList<MiruFilter>> subFilter;

    public MiruFilter(
        MiruFilterOperation operation,
        Optional<ImmutableList<MiruFieldFilter>> fieldFilters,
        Optional<ImmutableList<MiruFilter>> subFilter) {
        this.operation = operation;
        this.fieldFilters = fieldFilters;
        this.subFilter = subFilter;
    }

    @JsonCreator
    public static MiruFilter fromJson(
        @JsonProperty("operation") MiruFilterOperation operation,
        @JsonProperty("fieldFilters") List<MiruFieldFilter> fieldFilters,
        @JsonProperty("subFilter") List<MiruFilter> subFilter) {
        return new MiruFilter(
            operation,
            Optional.fromNullable(fieldFilters).transform(MiruFilter.<MiruFieldFilter>toImmutableList()),
            Optional.fromNullable(subFilter).transform(MiruFilter.<MiruFilter>toImmutableList()));
    }

    @JsonGetter("fieldFilters")
    public List<MiruFieldFilter> getFieldFiltersNullable() {
        return fieldFilters.orNull();
    }

    @JsonGetter("subFilter")
    public List<MiruFilter> getSubFilterNullable() {
        return subFilter.orNull();
    }

    @Override
    public String toString() {
        return "MiruFilter{" +
            "operation=" + operation +
            ", fieldFilters=" + fieldFilters +
            ", subFilter=" + subFilter +
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

        if (fieldFilters != null ? !fieldFilters.equals(that.fieldFilters) : that.fieldFilters != null) {
            return false;
        }
        if (operation != that.operation) {
            return false;
        }
        if (subFilter != null ? !subFilter.equals(that.subFilter) : that.subFilter != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = operation != null ? operation.hashCode() : 0;
        result = 31 * result + (fieldFilters != null ? fieldFilters.hashCode() : 0);
        result = 31 * result + (subFilter != null ? subFilter.hashCode() : 0);
        return result;
    }

    private static final <T> Function<List<T>, ImmutableList<T>> toImmutableList() {
        return new Function<List<T>, ImmutableList<T>>() {
            @Override
            public ImmutableList<T> apply(List<T> input) {
                if (input == null) {
                    return null;
                }
                return ImmutableList.copyOf(input);
            }
        };
    }
}
