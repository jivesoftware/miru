package com.jivesoftware.os.miru.stream.plugins.benchmark.caliper;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Taken from Google Caliper so we can upload our own results
 */
public class Value {
    static final Value DEFAULT = new Value();

    public static Value create(double value, String unit) {
        return new Value(value, checkNotNull(unit));
    }

    private double magnitude;
    // TODO(gak): do something smarter than string for units
    // TODO(gak): give guidelines for how to specify units.  E.g. s or seconds
    private String unit;

    private Value() {
        this.magnitude = 0.0;
        this.unit = "";
    }

    private Value(double value, String unit) {
        this.magnitude = value;
        this.unit = unit;
    }

    public String unit() {
        return unit;
    }

    public double magnitude() {
        return magnitude;
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Value) {
            Value that = (Value) obj;
            return this.magnitude == that.magnitude
                && this.unit.equals(that.unit);
        } else {
            return false;
        }
    }

    @Override public int hashCode() {
        return Objects.hashCode(magnitude, unit);
    }

    @Override public String toString() {
        return new StringBuilder().append(magnitude).append(unit).toString();
    }
}
