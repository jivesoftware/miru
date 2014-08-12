package com.jivesoftware.os.miru.client;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import java.util.List;

/**
 *
 * @author jonathan
 */
public interface MiruActivitySenderProvider {

    static public interface MiruActivitySender {

        void send(List<MiruPartitionedActivity> activities);
    }

    MiruActivitySender get(MiruHost host);
}
