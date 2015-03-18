package com.jivesoftware.os.miru.api.wal;

/**
 *
 * @author jonathan.colt
 */
public class Sip implements Comparable<Sip> {
    public static final Sip INITIAL = new Sip(0, 0);
    public final long clockTimestamp;
    public final long activityTimestamp;

    public Sip(long clockTimestamp, long activityTimestamp) {
        this.clockTimestamp = clockTimestamp;
        this.activityTimestamp = activityTimestamp;
    }

    @Override
    public int compareTo(Sip o) {
        int c = Long.compare(clockTimestamp, o.clockTimestamp);
        if (c == 0) {
            c = Long.compare(activityTimestamp, o.activityTimestamp);
        }
        return c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Sip sip = (Sip) o;
        if (activityTimestamp != sip.activityTimestamp) {
            return false;
        }
        if (clockTimestamp != sip.clockTimestamp) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (clockTimestamp ^ (clockTimestamp >>> 32));
        result = 31 * result + (int) (activityTimestamp ^ (activityTimestamp >>> 32));
        return result;
    }

}
