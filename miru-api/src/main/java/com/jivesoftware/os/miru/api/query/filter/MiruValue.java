package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;

/**
 *
 */
public class MiruValue {

    public final String[] parts;

    @JsonCreator
    public MiruValue(@JsonProperty("parts") String... parts) {
        this.parts = parts;
    }

    public String last() {
        return parts[parts.length - 1];
    }

    public String[] slice(int offset, int length) {
        String[] sliced = new String[length];
        System.arraycopy(parts, offset, sliced, 0, length);
        return sliced;
    }

    @Override
    public String toString() {
        return "MiruValue{" +
            "parts=" + Arrays.toString(parts) +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruValue miruValue = (MiruValue) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(parts, miruValue.parts);

    }

    @Override
    public int hashCode() {
        return parts != null ? Arrays.hashCode(parts) : 0;
    }
}
