package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;

import java.util.*;

/**
 * @author jonathan
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MiruActivity {

    public final MiruTenantId tenantId;
    public final long time; // orderIdProvider, instead os System.currentime, sortable, timerangeable
    public final String[] authz; // same entitlement foo as in Sensei
    public final long version;
    public final MiruTermId[][] fieldsValues;
    public final Map<String, MiruIBA[]> propsValues;

    private MiruActivity(MiruTenantId tenantId, long time, String[] authz, long version, MiruTermId[][] fieldsValues,
            Map<String, MiruIBA[]> propsValues) {
        this.tenantId = tenantId;
        this.time = time;
        this.authz = authz;
        this.version = version;
        this.fieldsValues = fieldsValues;
        this.propsValues = Collections.unmodifiableMap(propsValues);
    }

    @JsonCreator
    public static MiruActivity fromJson(
            @JsonProperty("tenantId") byte[] tenantId,
            @JsonProperty("time") long time,
            @JsonProperty("authz") String[] authz,
            @JsonProperty("version") long version,
            @JsonProperty("fieldsValues") MiruTermId[][] fieldsValues,
            @JsonProperty("propsValues") Map<String, MiruIBA[]> propsValues) {
        return new MiruActivity(new MiruTenantId(tenantId), time, authz, version, fieldsValues, propsValues);
    }

    @JsonGetter("tenantId")
    public byte[] getTenantIdAsBytes() {
        return tenantId.getBytes();
    }

    public Map<String, List<MiruIBA>> getPropsValues() {
        return Maps.transformValues(propsValues, new Function<MiruIBA[], List<MiruIBA>>() {
            @Override
            public List<MiruIBA> apply(MiruIBA[] input) {
                return Lists.newArrayList(input);
            }
        });
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruActivity activity = (MiruActivity) o;

        if (time != activity.time) {
            return false;
        }
        if (version != activity.version) {
            return false;
        }
        if (!Arrays.equals(authz, activity.authz)) {
            return false;
        }
        if (!Arrays.deepEquals(fieldsValues, activity.fieldsValues)) {
            return false;
        }
        if (!getPropsValues().equals(activity.getPropsValues())) {
            return false;
        }
        if (tenantId != null ? !tenantId.equals(activity.tenantId) : activity.tenantId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantId != null ? tenantId.hashCode() : 0;
        result = 31 * result + (int) (time ^ (time >>> 32));
        result = 31 * result + (authz != null ? Arrays.hashCode(authz) : 0);
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + Arrays.deepHashCode(fieldsValues);
        result = 31 * result + propsValues.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MiruActivity{" +
                "tenantId=" + tenantId +
                ", time=" + time +
                ", authz=" + Arrays.toString(authz) +
                ", version=" + version +
                ", fieldsValues=" + fieldsAsString() +
                ", propsValues=" + propsAsString() +
                '}';
    }

    public String fieldsAsString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < fieldsValues.length; i++) {
            if (fieldsValues[i] == null || fieldsValues[i].length == 0) {
                continue;
            }
            sb.append(i).append("=[");
            for (MiruIBA value : fieldsValues[i]) {
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

    public String propsAsString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (Map.Entry<String, MiruIBA[]> entry : propsValues.entrySet()) {
            sb.append(entry.getKey()).append("=[");
            for (MiruIBA value : entry.getValue()) {
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

    public static final class Builder {

        private final MiruSchema schema;
        private final MiruTenantId tenantId;
        private final long time;
        private final String[] authz;
        private final long version;
        private final MiruTermId[][] fieldsValues;
        private final Map<String, List<MiruIBA>> propsValues = Maps.newHashMap();

        public Builder(MiruSchema schema, MiruTenantId tenantId, long time, String[] authz, long version) {
            this.schema = schema;
            this.tenantId = tenantId;
            this.time = time;
            this.authz = authz;
            this.version = version;
            this.fieldsValues = new MiruTermId[schema.fieldCount()][];
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

        public Builder putPropValue(String prop, byte[] value) {
            getPropValues(prop).add(BYTES_TO_IBA.apply(value));
            return this;
        }

        public Builder putAllPropValues(String prop, Collection<byte[]> values) {
            getPropValues(prop).addAll(Collections2.transform(values, BYTES_TO_IBA));
            return this;
        }

        public Builder putPropsValues(Map<String, MiruIBA[]> propsValues) {
            for (Map.Entry<String, MiruIBA[]> entry : propsValues.entrySet()) {
                getPropValues(entry.getKey()).addAll(Arrays.asList(entry.getValue()));
            }
            return this;
        }

        private List<MiruIBA> getPropValues(String prop) {
            List<MiruIBA> propValues = propsValues.get(prop);
            if (propValues == null) {
                // most have 1 entry, far less expensive overall
                propValues = Lists.newArrayListWithCapacity(1);
                propsValues.put(prop, propValues);
            }
            return propValues;
        }

        public MiruActivity build() {
            return new MiruActivity(tenantId, time, authz, version, fieldsValues,
                    Maps.newHashMap(Maps.transformValues(propsValues, new Function<List<MiruIBA>, MiruIBA[]>() {
                        @Override
                        public MiruIBA[] apply(List<MiruIBA> input) {
                            return input.toArray(new MiruIBA[input.size()]);
                        }
                    })));
        }

        private static final Function<String, MiruTermId> STRING_TO_TERMID = new Function<String, MiruTermId>() {
            @Override
            public MiruTermId apply(String input) {
                return input != null ? new MiruTermId(input.getBytes(Charsets.UTF_8)) : null;
            }
        };

        private static final Function<byte[], MiruIBA> BYTES_TO_IBA = new Function<byte[], MiruIBA>() {
            @Override
            public MiruIBA apply(byte[] input) {
                return input != null ? new MiruIBA(input) : null;
            }
        };
    }
}
