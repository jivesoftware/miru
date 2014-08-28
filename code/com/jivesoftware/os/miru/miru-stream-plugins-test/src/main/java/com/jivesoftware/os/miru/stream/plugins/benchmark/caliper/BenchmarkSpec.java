package com.jivesoftware.os.miru.stream.plugins.benchmark.caliper;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import java.util.Map;
import java.util.SortedMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Taken from Google Caliper so we can upload our own results
 */
public final class BenchmarkSpec {
    static final BenchmarkSpec DEFAULT = new BenchmarkSpec();

    private String className;
    private String methodName;
    private SortedMap<String, String> parameters;
    private int hash;

    private BenchmarkSpec() {
        this.className = "";
        this.methodName = "";
        this.parameters = Maps.newTreeMap();
    }

    private BenchmarkSpec(Builder builder) {
        this.className = builder.className;
        this.methodName = builder.methodName;
        this.parameters = Maps.newTreeMap(builder.parameters);
    }

    public String className() {
        return className;
    }

    public String methodName() {
        return methodName;
    }

    public ImmutableSortedMap<String, String> parameters() {
        return ImmutableSortedMap.copyOf(parameters);
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof BenchmarkSpec) {
            BenchmarkSpec that = (BenchmarkSpec) obj;
            return this.className.equals(that.className)
                && this.methodName.equals(that.methodName)
                && this.parameters.equals(that.parameters);
        } else {
            return false;
        }
    }

    private void initHash() {
        if (hash == 0) {
            this.hash = Hashing.murmur3_32()
                .hashObject(this, BenchmarkSpecFunnel.INSTANCE).asInt();
        }
    }

    @Override public int hashCode() {
        initHash();
        return hash;
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
            .add("className", className)
            .add("methodName", methodName)
            .add("parameters", parameters)
            .toString();
    }

    enum BenchmarkSpecFunnel implements Funnel<BenchmarkSpec> {
        INSTANCE;

        @Override public void funnel(BenchmarkSpec from, PrimitiveSink into) {
            into.putString(from.className)
                .putString(from.methodName);
            StringMapFunnel.INSTANCE.funnel(from.parameters, into);
        }
    }

    public static final class Builder {
        private String className;
        private String methodName;
        private final SortedMap<String, String> parameters = Maps.newTreeMap();

        public Builder className(String className) {
            this.className = checkNotNull(className);
            return this;
        }

        public Builder methodName(String methodName) {
            this.methodName = checkNotNull(methodName);
            return this;
        }

        public Builder addParameter(String parameterName, String value) {
            this.parameters.put(checkNotNull(parameterName), checkNotNull(value));
            return this;
        }

        public Builder addAllParameters(Map<String, String> parameters) {
            this.parameters.putAll(parameters);
            return this;
        }

        public BenchmarkSpec build() {
            checkState(className != null);
            checkState(methodName != null);
            return new BenchmarkSpec(this);
        }
    }
}