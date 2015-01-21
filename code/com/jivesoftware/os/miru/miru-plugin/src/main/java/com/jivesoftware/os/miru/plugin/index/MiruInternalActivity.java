package com.jivesoftware.os.miru.plugin.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
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

    public long sizeInBytes() {
        long sizeInBytes = tenantId.getBytes().length + 8 + 8;
        if (authz != null) {
            for (String a : authz) {
                sizeInBytes += a.getBytes(Charsets.UTF_8).length;
            }
        }

        // terms are interned and counted by field index
        sizeInBytes += fieldsValues.length * 8;
        for (MiruTermId[] fieldValues : fieldsValues) {
            if (fieldValues != null) {
                sizeInBytes += fieldValues.length * 8;
            }
        }

        return sizeInBytes;
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

        private final MiruSchema schema;
        private final MiruTenantId tenantId;
        private final long time;
        private final String[] authz;
        private final long version;
        private final MiruTermId[][] fieldsValues;
        private final MiruIBA[][] propsValues;

        public Builder(MiruSchema schema, MiruTenantId tenantId, long time, String[] authz, long version) {
            this.schema = schema;
            this.tenantId = tenantId;
            this.time = time;
            this.authz = authz;
            this.version = version;
            this.fieldsValues = new MiruTermId[schema.fieldCount()][];
            this.propsValues = new MiruIBA[schema.propertyCount()][];
        }

        public Builder putFieldValue(String field, String value) {
            int fieldId = schema.getFieldId(field);
            MiruTermId[] oldValues = this.fieldsValues[fieldId];
            if (oldValues == null || oldValues.length == 0) {
                this.fieldsValues[fieldId] = new MiruTermId[]{STRING_TO_TERMID.apply(value)};
            } else {
                MiruTermId[] newValues = new MiruTermId[oldValues.length + 1];
                System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);
                newValues[newValues.length - 1] = STRING_TO_TERMID.apply(value);
                this.fieldsValues[fieldId] = newValues;
            }
            return this;
        }

        public Builder putAllFieldValues(String field, MiruTermId[] values) {
            int fieldId = schema.getFieldId(field);
            MiruTermId[] oldValues = this.fieldsValues[fieldId];
            if (oldValues == null || oldValues.length == 0) {
                this.fieldsValues[fieldId] = values;
            } else {
                MiruTermId[] newValues = new MiruTermId[oldValues.length + values.length];
                System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);
                System.arraycopy(values, 0, newValues, oldValues.length, values.length);
                this.fieldsValues[fieldId] = newValues;
            }
            return this;
        }

        public Builder putAllFieldValues(String field, Collection<String> values) {
            return putAllFieldValues(field, Collections2.transform(values, STRING_TO_TERMID).toArray(new MiruTermId[values.size()]));
        }

        public Builder putFieldsValues(MiruTermId[][] fieldsValues) {
            System.arraycopy(fieldsValues, 0, this.fieldsValues, 0, fieldsValues.length);
            return this;
        }

        public Builder putFieldsValues(Map<String, MiruTermId[]> fieldsValues) {
            for (Map.Entry<String, MiruTermId[]> entry : fieldsValues.entrySet()) {
                putAllFieldValues(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public Builder putPropValue(String prop, String value) {
            int propId = schema.getPropertyId(prop);
            MiruIBA[] oldValues = this.propsValues[propId];
            if (oldValues == null || oldValues.length == 0) {
                this.propsValues[propId] = new MiruIBA[]{STRING_TO_IBA.apply(value)};
            } else {
                MiruIBA[] newValues = new MiruIBA[oldValues.length + 1];
                System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);
                newValues[newValues.length - 1] = STRING_TO_IBA.apply(value);
                this.propsValues[propId] = newValues;
            }
            return this;
        }

        public Builder putAllPropValues(String prop, MiruIBA[] values) {
            int propId = schema.getPropertyId(prop);
            MiruIBA[] oldValues = this.propsValues[propId];
            if (oldValues == null || oldValues.length == 0) {
                this.propsValues[propId] = values;
            } else {
                MiruIBA[] newValues = new MiruIBA[oldValues.length + values.length];
                System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);
                System.arraycopy(values, 0, newValues, oldValues.length, values.length);
                this.propsValues[propId] = newValues;
            }
            return this;
        }

        public Builder putAllPropValues(String field, Collection<String> values) {
            return putAllPropValues(field, Collections2.transform(values, STRING_TO_IBA).toArray(new MiruIBA[values.size()]));
        }

        public Builder putPropsValues(MiruIBA[][] propsValues) {
            System.arraycopy(propsValues, 0, this.propsValues, 0, propsValues.length);
            return this;
        }

        public Builder putPropsValues(Map<String, MiruIBA[]> propsValues) {
            for (Map.Entry<String, MiruIBA[]> entry : propsValues.entrySet()) {
                putAllPropValues(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public MiruInternalActivity build() {
            return new MiruInternalActivity(tenantId, time, authz, version, fieldsValues, propsValues);
        }

        private static final Function<String, MiruTermId> STRING_TO_TERMID = new Function<String, MiruTermId>() {
            @Override
            public MiruTermId apply(String input) {
                return input != null ? new MiruTermId(input.getBytes(Charsets.UTF_8)) : null;
            }
        };

        private static final Function<String, MiruIBA> STRING_TO_IBA = new Function<String, MiruIBA>() {
            @Override
            public MiruIBA apply(String input) {
                return input != null ? new MiruIBA(input.getBytes(Charsets.UTF_8)) : null;
            }
        };
    }
}
