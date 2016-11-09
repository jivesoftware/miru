/*
 * Copyright 2014 Jive Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MiruActivity {

    public final MiruTenantId tenantId;
    public final long time; // orderIdProvider, instead os System.currentime, sortable, timerangeable
    public final long version;
    public final boolean realtimeDelivery;
    public final String[] authz; // same entitlement foo as in Sensei
    public final Map<String, List<String>> fieldsValues;
    public final Map<String, List<String>> propsValues;

    @JsonCreator
    public MiruActivity(
        @JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("time") long time,
        @JsonProperty("version") long version,
        @JsonProperty("realtimeDelivery") boolean realtimeDelivery,
        @JsonProperty("authz") String[] authz,
        @JsonProperty("fieldsValues") Map<String, List<String>> fieldsValues,
        @JsonProperty("propsValues") Map<String, List<String>> propsValues) {
        this.tenantId = tenantId;
        this.time = time;
        this.version = version;
        this.realtimeDelivery = realtimeDelivery;
        this.authz = authz;
        this.fieldsValues = fieldsValues;
        this.propsValues = propsValues;
    }

    public MiruActivity copyToTenantId(MiruTenantId toTenantId) {
        return new MiruActivity(toTenantId, time, version, realtimeDelivery, authz, fieldsValues, propsValues);
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
        return "MiruActivity{" +
            "tenantId=" + tenantId +
            ", time=" + time +
            ", version=" + version +
            ", realtimeDelivery=" + realtimeDelivery +
            ", authz=" + Arrays.toString(authz) +
            ", fieldsValues=" + fieldsValues +
            ", propsValues=" + propsValues +
            '}';
    }

    public static class Builder {

        private final MiruTenantId tenantId;
        private final long time;
        private final long version;
        private final boolean realtimeDelivery;
        private final String[] authz;
        private final Map<String, List<String>> fieldsValues = Maps.newHashMap();
        private final Map<String, List<String>> propsValues = Maps.newHashMap();

        public Builder(MiruTenantId tenantId, long time, long version, boolean realtimeDelivery, String[] authz) {
            this.tenantId = tenantId;
            this.time = time;
            this.version = version;
            this.realtimeDelivery = realtimeDelivery;
            this.authz = authz;
        }

        public Builder putFieldValue(String field, String value) {
            getFieldValues(field).add(value);
            return this;
        }

        public Builder putAllFieldValues(String field, Collection<String> value) {
            getFieldValues(field).addAll(value);
            return this;
        }

        public Builder putFieldsValues(Map<String, List<String>> fieldsValues) {
            for (Map.Entry<String, List<String>> entry : fieldsValues.entrySet()) {
                getFieldValues(entry.getKey()).addAll(entry.getValue());
            }
            return this;
        }

        private List<String> getFieldValues(String field) {
            List<String> fieldValues = fieldsValues.get(field);
            if (fieldValues == null) {
                fieldValues = Lists.newLinkedList();
                fieldsValues.put(field, fieldValues);
            }
            return fieldValues;
        }

        public Builder putPropValue(String prop, String value) {
            getPropValues(prop).add(value);
            return this;
        }

        public Builder putAllPropValues(String prop, Collection<String> value) {
            getPropValues(prop).addAll(value);
            return this;
        }

        public Builder putPropsValues(Map<String, List<String>> propsValues) {
            for (Map.Entry<String, List<String>> entry : propsValues.entrySet()) {
                getPropValues(entry.getKey()).addAll(entry.getValue());
            }
            return this;
        }

        private List<String> getPropValues(String prop) {
            List<String> propValues = propsValues.get(prop);
            if (propValues == null) {
                propValues = Lists.newLinkedList();
                propsValues.put(prop, propValues);
            }
            return propValues;
        }

        public MiruActivity build() {
            return new MiruActivity(tenantId, time, version, realtimeDelivery, authz, ImmutableMap.copyOf(fieldsValues), ImmutableMap.copyOf(propsValues));
        }

    }

}
