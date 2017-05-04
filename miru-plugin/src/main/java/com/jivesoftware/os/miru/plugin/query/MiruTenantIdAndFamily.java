package com.jivesoftware.os.miru.plugin.query;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 * Created by jonathan.colt on 5/4/17.
 */
class MiruTenantIdAndFamily {
    public final MiruTenantId miruTenantId;
    public final String family;

    public MiruTenantIdAndFamily(MiruTenantId miruTenantId, String family) {
        this.miruTenantId = miruTenantId;
        this.family = family;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MiruTenantIdAndFamily)) {
            return false;
        }

        MiruTenantIdAndFamily that = (MiruTenantIdAndFamily) o;

        if (!miruTenantId.equals(that.miruTenantId)) {
            return false;
        }
        return family.equals(that.family);

    }

    @Override
    public int hashCode() {
        int result = miruTenantId.hashCode();
        result = 31 * result + family.hashCode();
        return result;
    }
}
