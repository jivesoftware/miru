package com.jivesoftware.os.miru.stream.plugins.benchmark.caliper;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Taken from Google Caliper so we can upload our own results
 */
public final class Trial { // used to be Result
    public static final Trial DEFAULT = new Trial();

    private UUID id;
    private Run run;
    private InstrumentSpec instrumentSpec;
    private Scenario scenario;
    private List<Measurement> measurements;

    private Trial() {
        this.id = new UUID(0L, 0L);
        this.run = Run.DEFAULT;
        this.instrumentSpec = InstrumentSpec.DEFAULT;
        this.scenario = Scenario.DEFAULT;
        this.measurements = Lists.newArrayList();
    }

    private Trial(Builder builder) {
        this.id = builder.id;
        this.run = builder.run;
        this.instrumentSpec = builder.instrumentSpec;
        this.scenario = builder.scenario;
        this.measurements = Lists.newArrayList(builder.measurements);
    }

    public UUID id() {
        return id;
    }

    public Run run() {
        return run;
    }

    public InstrumentSpec instrumentSpec() {
        return instrumentSpec;
    }

    public Scenario scenario() {
        return scenario;
    }

    public ImmutableList<Measurement> measurements() {
        return ImmutableList.copyOf(measurements);
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Trial) {
            Trial that = (Trial) obj;
            return this.id.equals(that.id)
                && this.run.equals(that.run)
                && this.instrumentSpec.equals(that.instrumentSpec)
                && this.scenario.equals(that.scenario)
                && this.measurements.equals(that.measurements);
        } else {
            return false;
        }
    }

    @Override public int hashCode() {
        return Objects.hashCode(id, run, instrumentSpec, scenario, measurements);
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
            .add("id", id)
            .add("run", run)
            .add("instrumentSpec", instrumentSpec)
            .add("scenario", scenario)
            .add("measurements", measurements)
            .toString();
    }

    public static final class Builder {
        private final UUID id;
        private Run run;
        private InstrumentSpec instrumentSpec;
        private Scenario scenario;
        private final List<Measurement> measurements = Lists.newArrayList();

        public Builder(UUID id) {
            this.id = checkNotNull(id);
        }

        public Builder run(Run.Builder runBuilder) {
            return run(runBuilder.build());
        }

        public Builder run(Run run) {
            this.run = checkNotNull(run);
            return this;
        }

        public Builder instrumentSpec(InstrumentSpec.Builder instrumentSpecBuilder) {
            return instrumentSpec(instrumentSpecBuilder.build());
        }

        public Builder instrumentSpec(InstrumentSpec instrumentSpec) {
            this.instrumentSpec = checkNotNull(instrumentSpec);
            return this;
        }

        public Builder scenario(Scenario.Builder scenarioBuilder) {
            return scenario(scenarioBuilder.build());
        }

        public Builder scenario(Scenario scenario) {
            this.scenario = checkNotNull(scenario);
            return this;
        }

        public Builder addMeasurement(Measurement.Builder measurementBuilder) {
            return addMeasurement(measurementBuilder.build());
        }

        public Builder addMeasurement(Measurement measurement) {
            this.measurements.add(measurement);
            return this;
        }

        public Builder addAllMeasurements(Iterable<Measurement> measurements) {
            Iterables.addAll(this.measurements, measurements);
            return this;
        }

        public Trial build() {
            checkState(run != null);
            checkState(instrumentSpec != null);
            checkState(scenario != null);
            return new Trial(this);
        }
    }
}
