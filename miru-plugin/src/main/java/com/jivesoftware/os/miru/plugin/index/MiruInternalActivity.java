package com.jivesoftware.os.miru.plugin.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Arrays;

/**
 * @author jonathan
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MiruInternalActivity {

    public final MiruTenantId tenantId;
    public final long time;
    public final long version;
    public final String[] authz;
    public final MiruTermId[][] fieldsValues;
    public final MiruIBA[][] propsValues;

    public MiruInternalActivity(MiruTenantId tenantId, long time, long version, String[] authz, MiruTermId[][] fieldsValues, MiruIBA[][] propsValues) {
        this.tenantId = tenantId;
        this.time = time;
        this.authz = authz;
        this.version = version;
        this.fieldsValues = fieldsValues;
        this.propsValues = propsValues;
    }

    @JsonCreator
    public static MiruInternalActivity fromJson(
        @JsonProperty("tenantId") byte[] tenantId,
        @JsonProperty("time") long time,
        @JsonProperty("version") long version,
        @JsonProperty("authz") String[] authz,
        @JsonProperty("fieldsValues") MiruTermId[][] fieldsValues,
        @JsonProperty("propsValues") MiruIBA[][] propsValues) {
        return new MiruInternalActivity(new MiruTenantId(tenantId), time, version, authz, fieldsValues, propsValues);
    }

    @JsonGetter("tenantId")
    public byte[] getTenantIdAsBytes() {
        return tenantId.getBytes();
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public String toString() {
        return "MiruInternalActivity{" +
            "tenantId=" + tenantId +
            ", time=" + time +
            ", version=" + version +
            ", authz=" + Arrays.toString(authz) +
            ", fieldsValues=" + Arrays.deepToString(fieldsValues) +
            ", propsValues=" + Arrays.deepToString(propsValues) +
            '}';
    }

    public static class Builder {

        private final MiruTenantId tenantId;
        private final long time;
        private final long version;
        private final String[] authz;
        private final MiruTermId[][] fieldsValues;
        private final MiruIBA[][] propsValues;

        public Builder(MiruSchema schema, MiruTenantId tenantId, long time, long version, String[] authz) {
            this.tenantId = tenantId;
            this.time = time;
            this.version = version;
            this.authz = authz;
            this.fieldsValues = new MiruTermId[schema.fieldCount()][];
            this.propsValues = new MiruIBA[schema.propertyCount()][];
        }

        public Builder putFieldsValues(MiruTermId[][] fieldsValues) {
            System.arraycopy(fieldsValues, 0, this.fieldsValues, 0, fieldsValues.length);
            return this;
        }

        public Builder putPropsValues(MiruIBA[][] propsValues) {
            System.arraycopy(propsValues, 0, this.propsValues, 0, propsValues.length);
            return this;
        }

        public MiruInternalActivity build() {
            return new MiruInternalActivity(tenantId, time, version, authz, fieldsValues, propsValues);
        }
    }
}
