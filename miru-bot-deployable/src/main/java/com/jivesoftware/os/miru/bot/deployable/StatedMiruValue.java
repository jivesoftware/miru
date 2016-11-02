package com.jivesoftware.os.miru.bot.deployable;

import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Random;

class StatedMiruValue {

    enum State {
        UNKNOWN,
        WRITTEN,
        READ_FAIL,
        READ_SUCCESS
    }

    private static final Random RAND = new Random();

    MiruValue value;
    State state;

    StatedMiruValue(
            MiruValue value,
            State state) {
        this.value = value;
        this.state = state;
    }

    static StatedMiruValue birth(int valueSizeFactor) {
        return new StatedMiruValue(
                new MiruValue(RandomStringUtils.randomAlphanumeric(RAND.nextInt(valueSizeFactor) + 1)),
                State.UNKNOWN);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatedMiruValue that = (StatedMiruValue) o;

        return value.equals(that.value);

    }

    public int hashCode() {
        return value.hashCode();
    }

    public String toString() {
        return "StatedMiruValue{" +
                "value=" + value +
                ", state=" + state +
                '}';
    }

}
