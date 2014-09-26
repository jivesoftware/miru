package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ComparisonChain;
import java.util.Iterator;

/** @author jonathan */
public class MiruHost implements Comparable<MiruHost> {

    // TODO add notion of rack so that host can be have the rink sort in a rack aware order.
    private final String logicalName;
    private final int port;

    @JsonCreator
    public MiruHost(@JsonProperty("logicalName") String logicalName, @JsonProperty("port") int port) {
        this.logicalName = logicalName;
        this.port = port;
    }

    public MiruHost(String stringForm) {
        Iterator<String> parts = Splitter.on(':').split(stringForm).iterator();
        this.logicalName = parts.next();
        this.port = Integer.parseInt(parts.next());
    }

    public String getLogicalName() {
        return logicalName;
    }

    public int getPort() {
        return port;
    }

    public String toStringForm() {
        return Joiner.on(':').join(logicalName, port);
    }

    @Override
    public String toString() {
        return toStringForm();
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

        if (port != miruHost.port) {
            return false;
        }
        if (logicalName != null ? !logicalName.equals(miruHost.logicalName) : miruHost.logicalName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = logicalName != null ? logicalName.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

    @Override
    public int compareTo(MiruHost o) {
        return ComparisonChain
            .start()
            .compare(logicalName, o.logicalName)
            .compare(port, o.port)
            .result();
    }

}
