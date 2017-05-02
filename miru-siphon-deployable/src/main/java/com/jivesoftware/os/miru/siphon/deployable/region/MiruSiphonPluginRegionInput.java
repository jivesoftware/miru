package com.jivesoftware.os.miru.siphon.deployable.region;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public class MiruSiphonPluginRegionInput {

    final long uniqueId;
    final String name;
    final String description;
    final String siphonPluginName;
    final String ringName;
    final String partitionName;
    final String destinationTenantId;
    final int batchSize;
    final String action;


    public MiruSiphonPluginRegionInput(long uniqueId,
        String name,
        String description,
        String siphonPluginName,
        String ringName,
        String partitionName,
        String destinationTenantId,
        int batchSize,
        String action) {

        this.uniqueId = uniqueId;
        this.name = name;
        this.description = description;
        this.siphonPluginName = siphonPluginName;
        this.ringName = ringName;
        this.partitionName = partitionName;
        this.destinationTenantId = destinationTenantId;
        this.batchSize = batchSize;
        this.action = action;
    }
}
