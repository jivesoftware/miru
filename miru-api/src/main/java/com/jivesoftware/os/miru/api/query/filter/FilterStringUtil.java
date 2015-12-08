package com.jivesoftware.os.miru.api.query.filter;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class FilterStringUtil {

    /**
     * Example input: activityType:0|1|2, authors:3765
     */
    public MiruFilter parse(String filtersString) {
        filtersString = filtersString == null ? null : filtersString.trim();
        if (filtersString == null || filtersString.isEmpty()) {
            return MiruFilter.NO_FILTER;
        }

        String[] filtersArray = filtersString.split("\\s*,\\s*");

        List<MiruFieldFilter> fieldFilters = Lists.newArrayList();

        for (String filterString : filtersArray) {
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

            fieldFilters.add(MiruFieldFilter.ofTerms(fieldType, fieldName, values));
        }

        return new MiruFilter(MiruFilterOperation.and, false, fieldFilters, Arrays.<MiruFilter>asList());
    }

}
