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
        filtersString = filtersString.trim();
        if (filtersString.isEmpty()) {
            return MiruFilter.NO_FILTER;
        }

        String[] filtersArray = filtersString.split("\\s*,\\s*");

        List<MiruFieldFilter> fieldFilters = Lists.newArrayList();

        for (String filterString : filtersArray) {
            String[] filterTokens = filterString.trim().split(":");
            String fieldName = filterTokens[0].trim();
            String[] values = filterTokens[1].split("\\s*\\|\\s*");
            fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, fieldName, Arrays.asList(values)));
        }

        return new MiruFilter(MiruFilterOperation.and, false, fieldFilters, Arrays.<MiruFilter>asList());
    }

}
