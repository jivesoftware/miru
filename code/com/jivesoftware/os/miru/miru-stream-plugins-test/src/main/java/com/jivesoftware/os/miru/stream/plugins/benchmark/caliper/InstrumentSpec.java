package com.jivesoftware.os.miru.stream.plugins.benchmark.caliper;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import java.util.Map;
import java.util.SortedMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Taken from Google Caliper so we can upload our own results
 */
public final class InstrumentSpec {
    static final InstrumentSpec DEFAULT = new InstrumentSpec();

    private int id;
    private String className;
    private SortedMap<String, String> options;
    private int hash;

    private InstrumentSpec() {
        this.className = "";
        this.options = Maps.newTreeMap();
    }

    private InstrumentSpec(Builder builder) {
        this.className = builder.className;
        this.options = Maps.newTreeMap(builder.options);
    }

    public String className() {
        return className;
    }

    public ImmutableSortedMap<String, String> options() {
        return ImmutableSortedMap.copyOf(options);
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof InstrumentSpec) {
            InstrumentSpec that = (InstrumentSpec) obj;
            return this.className.equals(that.className)
                && this.options.equals(that.options);
        } else {
            return false;
        }
    }

    private void initHash() {
        if (hash == 0) {
            this.hash = Hashing.murmur3_32()
                .newHasher()
                .putString(className)
                .putObject(options, StringMapFunnel.INSTANCE)
                .hash().asInt();
        }
    }

    @Override public int hashCode() {
        initHash();
        return hash;
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
            .add("className", className)
            .add("options", options)
            .toString();
    }

    public static final class Builder {
        private String className;
        private final SortedMap<String, String> options = Maps.newTreeMap();

        public Builder className(String className) {
            this.className = checkNotNull(className);
            return this;
        }

        public Builder instrumentClass(Class<?> insturmentClass) {
            return className(insturmentClass.getName());
        }

        public Builder addOption(String option, String value) {
            this.options.put(option, value);
            return this;
        }

        public Builder addAllOptions(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        public InstrumentSpec build() {
            checkState(className != null);
            return new InstrumentSpec(this);
        }
    }
}