package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import java.util.Map;

/**
*
*/
public class MiruTenantConfig {

    private final Map<MiruTenantConfigFields, Long> config;

    public MiruTenantConfig(Map<MiruTenantConfigFields, Long> config) {
        this.config = config;
    }

    public long getLong(MiruTenantConfigFields key, long defaultValue) {
        return Optional.fromNullable(config.get(key)).or(defaultValue);
    }

    public int getInt(MiruTenantConfigFields key, int defaultValue) {
        return (int) Optional.fromNullable(config.get(key)).or((long) defaultValue).longValue();
    }
}
