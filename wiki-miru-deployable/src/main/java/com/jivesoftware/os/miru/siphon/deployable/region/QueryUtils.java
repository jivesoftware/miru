package com.jivesoftware.os.miru.siphon.deployable.region;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class QueryUtils {

    private static final SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();

    // e.g. mpb=10,000, current=24,478, modulus=4,478, ceiling=30,000
    public static MiruTimeRange toMiruTimeRange(
        int smallestTime, TimeUnit smallestTimeUnit,
        int largestTime, TimeUnit largestTimeUnit,
        int buckets) {
        long rangeMillis = smallestTimeUnit.toMillis(smallestTime) - largestTimeUnit.toMillis(largestTime);
        long millisPerBucket = rangeMillis / buckets;
        long jiveLargestTime = new JiveEpochTimestampProvider().getTimestamp() - largestTimeUnit.toMillis(largestTime);
        long jiveModulusTime = jiveLargestTime % millisPerBucket;
        long jiveCeilingTime = jiveLargestTime - jiveModulusTime + millisPerBucket;
        final long packCeilingTime = snowflakeIdPacker.pack(jiveCeilingTime, 0, 0);
        final long packLookbackTime = packCeilingTime - snowflakeIdPacker.pack(rangeMillis, 0, 0);
        return new MiruTimeRange(packLookbackTime, packCeilingTime);
    }


    public static void addFieldFilter(List<MiruFieldFilter> fieldFilters, List<MiruFieldFilter> notFilters, String fieldName, String values) {
        if (values != null) {
            values = values.trim();
            String[] valueArray = values.split("\\s*,\\s*");
            List<String> terms = Lists.newArrayList();
            List<String> notTerms = Lists.newArrayList();
            for (String value : valueArray) {
                String trimmed = value.trim();
                if (!trimmed.isEmpty()) {
                    if (trimmed.startsWith("!")) {
                        if (trimmed.length() > 1) {
                            notTerms.add(trimmed.substring(1));
                        }
                    } else {
                        terms.add(trimmed);
                    }
                }
            }
            if (!terms.isEmpty()) {
                fieldFilters.add(MiruFieldFilter.of(MiruFieldType.primary, fieldName, terms));
            }
            if (!notTerms.isEmpty()) {
                notFilters.add(MiruFieldFilter.of(MiruFieldType.primary, fieldName, notTerms));
            }
        }
    }
}
