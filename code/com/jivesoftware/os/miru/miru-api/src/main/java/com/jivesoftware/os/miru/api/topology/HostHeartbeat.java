package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruHost;

/**
 *
 * @author jonathan.colt
 */
public class HostHeartbeat {

    public final MiruHost host;
    public final long heartbeat;

    public HostHeartbeat(MiruHost host, long heartbeat) {
        this.host = host;
        this.heartbeat = heartbeat;
    }

    // only host contributes to equals()
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HostHeartbeat that = (HostHeartbeat) o;

        return !(host != null ? !host.equals(that.host) : that.host != null);
    }

    // only host contributes to hashCode()
    @Override
    public int hashCode() {
        return host != null ? host.hashCode() : 0;
    }

}
