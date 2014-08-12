package com.jivesoftware.os.miru.service.partition;

/**
*
*/
public class TimeAndVersion {

    public final long timestamp;
    public final long version;

    TimeAndVersion(long timestamp, long version) {
        this.timestamp = timestamp;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeAndVersion that = (TimeAndVersion) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (version != that.version) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }
}
