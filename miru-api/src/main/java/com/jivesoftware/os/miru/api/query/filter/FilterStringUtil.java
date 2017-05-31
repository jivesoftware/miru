package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class FilterStringUtil {

    private final ObjectMapper mapper;

    public FilterStringUtil(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Example input: activityType:0|1|2, authors:3765
     */
    public MiruFilter parseFilters(String filtersString) throws IOException {
        filtersString = filtersString == null ? null : filtersString.trim();
        if (filtersString == null || filtersString.isEmpty()) {
            return MiruFilter.NO_FILTER;
        }

        if (filtersString.startsWith("{") && filtersString.endsWith("}")) {
            return mapper.readValue(filtersString, MiruFilter.class);
        }

        String[] filtersArray = filtersString.split("\\s*,\\s*");

        List<MiruFieldFilter> fieldFilters = Lists.newArrayList();

        for (String filterString : filtersArray) {
            MiruFieldFilter filter = parseFilter(filterString);
            if (filter != null) {
                fieldFilters.add(filter);
            }
        }

        return new MiruFilter(MiruFilterOperation.and, false, fieldFilters, Arrays.<MiruFilter>asList());
    }

    public MiruFieldFilter parseFilter(String filterString) {
        filterString = filterString == null ? null : filterString.trim();
        if (filterString == null || filterString.isEmpty()) {
            return null;
        }

        String[] filterTokens = filterString.trim().split(":");
        String fieldName = filterTokens[0].trim();
        String[] values = filterTokens[1].split("\\s*\\|\\s*");
        MiruFieldType fieldType = MiruFieldType.primary;
        if (fieldName.startsWith("~")) {
            fieldName = fieldName.substring(1);
            fieldType = MiruFieldType.latest;
        } else if (fieldName.startsWith("^")) {
            fieldName = fieldName.substring(1);
            fieldType = MiruFieldType.pairedLatest;
        } else if (fieldName.startsWith("%")) {
            fieldName = fieldName.substring(1);
            fieldType = MiruFieldType.bloom;
        }
        MiruFieldFilter filter = MiruFieldFilter.ofTerms(fieldType, fieldName, values);
        return filter;
    }

    public List<MiruValue> buildFieldPrefixes(List<String> inputFieldPrefixes) {
        List<MiruValue> fieldPrefixes = null;
        if (inputFieldPrefixes != null) {
            fieldPrefixes = Lists.newArrayListWithCapacity(inputFieldPrefixes.size());
            for (String prefix : inputFieldPrefixes) {
                String[] parts = prefix.trim().split("\\+");
                if (parts.length > 0 && !parts[0].isEmpty()) {
                    fieldPrefixes.add(new MiruValue(parts));
                }
            }
        }
        return fieldPrefixes;
    }
}
