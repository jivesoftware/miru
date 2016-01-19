package com.jivesoftware.os.miru.cluster;

import com.jivesoftware.os.miru.api.MiruHost;

/**
 *
 */
public interface HostDescriptorProvider {

    void stream(DescriptorStream stream) throws Exception;

    interface DescriptorStream {
        boolean descriptor(String datacenter, String rack, MiruHost host) throws Exception;
    }
}
