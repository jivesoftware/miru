package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public class AmzaSiphonerConfig {

    public long uniqueId;
    public String name;
    public String description;
    public String siphonPluginName;
    public PartitionName partitionName;
    public MiruTenantId destinationTenantId;
    public int batchSize;
}
