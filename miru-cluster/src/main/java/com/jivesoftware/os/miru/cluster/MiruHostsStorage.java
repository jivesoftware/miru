package com.jivesoftware.os.miru.cluster;

import com.jivesoftware.os.miru.api.MiruHost;
import java.util.List;

public interface MiruHostsStorage {
    void add(MiruHost host);

    void remove(MiruHost host);

    List<MiruHost> allHosts();
}
