package com.jivesoftware.os.miru.api.topology;

import java.util.Map;

/**
 *
 */
public class MiruTenantConfig {

    public final Map<String, Long> config;

    public MiruTenantConfig(Map<String, Long> config) {
        this.config = config;
    }

    public long getLong(String key, long defaultValue) {
        Long got = config.get(key);
        return got == null ? defaultValue : got;
    }

    public int getInt(String key, int defaultValue) {
        Long got = config.get(key);
        return got == null ? defaultValue : got.intValue();
    }
}
