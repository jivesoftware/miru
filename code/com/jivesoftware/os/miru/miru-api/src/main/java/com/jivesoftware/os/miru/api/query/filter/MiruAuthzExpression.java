package com.jivesoftware.os.miru.api.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class MiruAuthzExpression implements Serializable {

    public static final MiruAuthzExpression NOT_PROVIDED = new MiruAuthzExpression(null);

    public final List<String> values;

    @JsonCreator
    public MiruAuthzExpression(@JsonProperty("values") List<String> values) {
        this.values = values;
    }

    public long sizeInBytes() {
        long sizeInBytes = 0;
        for (String value : values) {
            sizeInBytes += 2 * value.length();
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

        MiruAuthzExpression that = (MiruAuthzExpression) o;

        return !(values != null ? !values.equals(that.values) : that.values != null);
    }

    @Override
    public int hashCode() {
        return values != null ? values.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MiruAuthzExpression{" +
            "values=" + values +
            '}';
    }
}
