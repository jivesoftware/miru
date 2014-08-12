package com.jivesoftware.os.miru.service.benchmark.caliper;

import com.google.common.base.Objects;
import com.google.common.hash.Hashing;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Taken from Google Caliper so we can upload our own results
 */
public final class Scenario {
    static final Scenario DEFAULT = new Scenario();

    private BenchmarkSpec benchmarkSpec;

    private int hash;

    private Scenario() {
        this.benchmarkSpec = BenchmarkSpec.DEFAULT;
    }

    private Scenario(Builder builder) {
        this.benchmarkSpec = builder.benchmarkSpec;
    }

    public BenchmarkSpec benchmarkSpec() {
        return benchmarkSpec;
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Scenario) {
            Scenario that = (Scenario) obj;
            return this.benchmarkSpec.equals(that.benchmarkSpec);
        } else {
            return false;
        }
    }

    private void initHash() {
        if (hash == 0) {
            this.hash = Hashing.murmur3_32()
                .newHasher()
                .putObject(benchmarkSpec, BenchmarkSpec.BenchmarkSpecFunnel.INSTANCE)
                .hash().asInt();
        }
    }

    @Override public int hashCode() {
        initHash();
        return hash;
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
            .add("benchmarkSpec", benchmarkSpec)
            .toString();
    }

    public static final class Builder {
        private BenchmarkSpec benchmarkSpec;

        public Builder benchmarkSpec(BenchmarkSpec.Builder benchmarkSpecBuilder) {
            return benchmarkSpec(benchmarkSpecBuilder.build());
        }

        public Builder benchmarkSpec(BenchmarkSpec benchmarkSpec) {
            this.benchmarkSpec = checkNotNull(benchmarkSpec);
            return this;
        }

        public Scenario build() {
            checkState(benchmarkSpec != null);
            return new Scenario(this);
        }
    }
}