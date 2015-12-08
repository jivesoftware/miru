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
import java.util.Objects;

/**
 * @author jonathan
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MiruInternalActivity {

    public final MiruTenantId tenantId;
    public final long time; // orderIdProvider, instead os System.currentime, sortable, timerangeable
    public final String[] authz; // same entitlement foo as in Sensei
    public final long version;
    public final MiruTermId[][] fieldsValues;
    public final MiruIBA[][] propsValues;

    public MiruInternalActivity(MiruTenantId tenantId, long time, String[] authz, long version, MiruTermId[][] fieldsValues, MiruIBA[][] propsValues) {
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
        @JsonProperty("authz") String[] authz,
        @JsonProperty("version") long version,
        @JsonProperty("fieldsValues") MiruTermId[][] fieldsValues,
        @JsonProperty("propsValues") MiruIBA[][] propsValues) {
        return new MiruInternalActivity(new MiruTenantId(tenantId), time, authz, version, fieldsValues, propsValues);
    }

    @JsonGetter("tenantId")
    public byte[] getTenantIdAsBytes() {
        return tenantId.getBytes();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.tenantId);
        hash = 79 * hash + (int) (this.time ^ (this.time >>> 32));
        hash = 79 * hash + Arrays.deepHashCode(this.authz);
        hash = 79 * hash + (int) (this.version ^ (this.version >>> 32));
        hash = 79 * hash + Arrays.deepHashCode(this.fieldsValues);
        hash = 79 * hash + Arrays.deepHashCode(this.propsValues);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MiruInternalActivity other = (MiruInternalActivity) obj;
        if (!Objects.equals(this.tenantId, other.tenantId)) {
            return false;
        }
        if (this.time != other.time) {
            return false;
        }
        if (!Arrays.deepEquals(this.authz, other.authz)) {
            return false;
        }
        if (this.version != other.version) {
            return false;
        }
        if (!Arrays.deepEquals(this.fieldsValues, other.fieldsValues)) {
            return false;
        }
        return Arrays.deepEquals(this.propsValues, other.propsValues);
    }

    @Override
    public String toString() {
        return "MiruActivity{" +
            "tenantId=" + tenantId +
            ", time=" + time +
            ", authz=" + Arrays.toString(authz) +
            ", version=" + version +
            ", fieldsValues=" + valuesAsString(fieldsValues) +
            ", propsValues=" + valuesAsString(propsValues) +
            '}';
    }

    public <T extends MiruIBA> String valuesAsString(T[][] values) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null || values[i].length == 0) {
                continue;
            }
            sb.append(i).append("=[");
            for (MiruIBA value : values[i]) {
                byte[] byteValue = value.getBytes();
                String v = (byteValue.length == 4)
                    ? String.valueOf(FilerIO.bytesInt(byteValue)) : (byteValue.length == 8)
                    ? String.valueOf(FilerIO.bytesLong(byteValue)) : new String(byteValue, Charsets.UTF_8);
                sb.append(v).append(", ");
            }
            sb.append("], ");
        }
        sb.append(']');
        return sb.toString();
    }

    public static class Builder {

        private final MiruTenantId tenantId;
        private final long time;
        private final String[] authz;
        private final long version;
        private final MiruTermId[][] fieldsValues;
        private final MiruIBA[][] propsValues;

        public Builder(MiruSchema schema, MiruTenantId tenantId, long time, String[] authz, long version) {
            this.tenantId = tenantId;
            this.time = time;
            this.authz = authz;
            this.version = version;
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
            return new MiruInternalActivity(tenantId, time, authz, version, fieldsValues, propsValues);
        }
    }
}
