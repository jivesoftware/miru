package com.jivesoftware.os.miru.stream.plugins.benchmark.caliper;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import java.util.Map;

/**
 * Taken from Google Caliper so we can upload our own results
 */
enum StringMapFunnel implements Funnel<Map<String, String>> {
    INSTANCE;

    @Override
    public void funnel(Map<String, String> from, PrimitiveSink into) {
        for (Map.Entry<String, String> entry : from.entrySet()) {
            into.putString(entry.getKey())
                .putByte((byte) -1) // separate key and value
                .putString(entry.getValue());
        }
    }
}
