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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** @author jonathan */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MiruActivity {

    public final MiruTenantId tenantId;
    public final long time; // orderIdProvider, instead os System.currentime, sortable, timerangeable
    public final String[] authz; // same entitlement foo as in Sensei
    public final long version;
    public final Map<String, MiruTermId[]> fieldsValues;
    public final Map<String, MiruIBA[]> propsValues;

    private MiruActivity(MiruTenantId tenantId, long time, String[] authz, long version, Map<String, MiruTermId[]> fieldsValues,
        Map<String, MiruIBA[]> propsValues) {
        this.tenantId = tenantId;
        this.time = time;
        this.authz = authz;
        this.version = version;
        this.fieldsValues = Collections.unmodifiableMap(fieldsValues);
        this.propsValues = Collections.unmodifiableMap(propsValues);
    }

    @JsonCreator
    public static MiruActivity fromJson(
        @JsonProperty("tenantId") byte[] tenantId,
        @JsonProperty("time") long time,
        @JsonProperty("authz") String[] authz,
        @JsonProperty("version") long version,
        @JsonProperty("fieldsValues") Map<String, MiruTermId[]> fieldsValues,
        @JsonProperty("propsValues") Map<String, MiruIBA[]> propsValues) {
        Builder builder = new Builder(new MiruTenantId(tenantId), time, authz, version);
        if (fieldsValues != null) {
            builder.putFieldsValues(fieldsValues);
        }
        if (propsValues != null) {
            builder.putPropsValues(propsValues);
        }
        return builder.build();
    }

    @JsonGetter("tenantId")
    public byte[] getTenantIdAsBytes() {
        return tenantId.getBytes();
    }

    public Map<String, List<MiruTermId>> getFieldsValues() {
        return Maps.transformValues(fieldsValues, new Function<MiruTermId[], List<MiruTermId>>() {
            @Override
            public List<MiruTermId> apply(MiruTermId[] input) {
                return Lists.newArrayList(input);
            }
        });
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
        sizeInBytes += fieldsValues.size() * 16; // 2 refs
        for (Map.Entry<String, MiruTermId[]> entry : fieldsValues.entrySet()) {
            sizeInBytes += entry.getKey().length() * 2 + entry.getValue().length * 8;
            // terms are interned and counted by field index
        }
        sizeInBytes += propsValues.size() * 16; // 2 refs
        for (Map.Entry<String, MiruIBA[]> entry : propsValues.entrySet()) {
            sizeInBytes += entry.getKey().length() * 2 + entry.getValue().length * 8;
            for (MiruIBA prop : entry.getValue()) {
                sizeInBytes += prop.getBytes().length;
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
        if (!getFieldsValues().equals(activity.getFieldsValues())) {
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
        result = 31 * result + fieldsValues.hashCode();
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
            ", fieldsValues=" + valuesAsString(fieldsValues) +
            ", propsValues=" + valuesAsString(propsValues) +
            '}';
    }

    public <T extends MiruIBA> String valuesAsString(Map<String, T[]> mapOfValues) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (String k : mapOfValues.keySet()) {
            sb.append(k).append("=[");
            T[] values = mapOfValues.get(k);
            for (MiruIBA value : values) {
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
        private final MiruTenantId tenantId;
        private final long time;
        private final String[] authz;
        private final long version;
        private final Map<String, List<MiruTermId>> fieldsValues = Maps.newHashMap();
        private final Map<String, List<MiruIBA>> propsValues = Maps.newHashMap();

        public Builder(MiruTenantId tenantId, long time, String[] authz, long version) {
            this.tenantId = tenantId;
            this.time = time;
            this.authz = authz;
            this.version = version;
        }

        public Builder putFieldValue(String field, String value) {
            getFieldValues(field).add(STRING_TO_TERMID.apply(value));
            return this;
        }

        public Builder putAllFieldValues(String field, Collection<String> value) {
            getFieldValues(field).addAll(Collections2.transform(value, STRING_TO_TERMID));
            return this;
        }

        public Builder putFieldsValues(Map<String, MiruTermId[]> fieldsValues) {
            for (Map.Entry<String, MiruTermId[]> entry : fieldsValues.entrySet()) {
                getFieldValues(entry.getKey()).addAll(Arrays.asList(entry.getValue()));
            }
            return this;
        }

        private List<MiruTermId> getFieldValues(String field) {
            List<MiruTermId> fieldValues = fieldsValues.get(field);
            if (fieldValues == null) {
                // most have 1 entry, far less expensive overall
                fieldValues = Lists.newArrayList();
                fieldsValues.put(field, fieldValues);
            }
            return fieldValues;
        }

        public Builder putPropValue(String prop, byte[] value) {
            getPropValues(prop).add(BYTES_TO_IBA.apply(value));
            return this;
        }

        public Builder putAllPropValues(String prop, Collection<byte[]> value) {
            getPropValues(prop).addAll(Collections2.transform(value, BYTES_TO_IBA));
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
            return new MiruActivity(tenantId, time, authz, version,
                Maps.newHashMap(Maps.transformValues(fieldsValues, new Function<List<MiruTermId>, MiruTermId[]>() {
                    @Override
                    public MiruTermId[] apply(List<MiruTermId> input) {
                        return input.toArray(new MiruTermId[input.size()]);
                    }
                })),
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
