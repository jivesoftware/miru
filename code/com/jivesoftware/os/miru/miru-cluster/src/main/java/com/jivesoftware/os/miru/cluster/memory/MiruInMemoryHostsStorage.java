package com.jivesoftware.os.miru.cluster.memory;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruHostsStorage;

import java.util.List;

/** @author jonathan */
public class MiruInMemoryHostsStorage implements MiruHostsStorage {

    private final List<MiruHost> hosts;

    public MiruInMemoryHostsStorage() {
        this.hosts = Lists.newArrayList();
    }

    @Override
    public void add(MiruHost host) {
        hosts.add(host);
    }

    @Override
    public void remove(MiruHost host) {
        hosts.remove(host);
    }

    @Override
    public List<MiruHost> allHosts() {
        return hosts;
    }
}
