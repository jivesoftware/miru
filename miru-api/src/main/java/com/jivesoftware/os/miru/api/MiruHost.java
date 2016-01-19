package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** @author jonathan */
public class MiruHost implements Comparable<MiruHost> {

    private final String logicalName;

    public MiruHost(String logicalName) {
        this.logicalName = logicalName;
    }

    @JsonCreator
    public static MiruHost fromJson(@JsonProperty("logicalName") String logicalName,
        @JsonProperty("port") Integer port) {
        return new MiruHost(port != null ? (logicalName + ":" + port) : logicalName);
    }

    public String getLogicalName() {
        return logicalName;
    }

    @Override
    public String toString() {
        return getLogicalName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruHost miruHost = (MiruHost) o;

        return !(logicalName != null ? !logicalName.equals(miruHost.logicalName) : miruHost.logicalName != null);

    }

    @Override
    public int hashCode() {
        return logicalName != null ? logicalName.hashCode() : 0;
    }

    @Override
    public int compareTo(MiruHost o) {
        return logicalName.compareTo(o.logicalName);
    }

}
