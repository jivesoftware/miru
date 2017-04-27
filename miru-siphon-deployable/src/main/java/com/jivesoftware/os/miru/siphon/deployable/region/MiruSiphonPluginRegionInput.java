package com.jivesoftware.os.miru.siphon.deployable.region;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public class MiruSiphonPluginRegionInput {

    final String tenantId;
    final String query;
    final String folderGuids;
    final String userGuids;
    final String querier;
    final int numberOfResult;
    final boolean wildcardExpansion;

    public MiruSiphonPluginRegionInput(String tenantId,
        String query,
        String folderIds,
        String userIds,
        String querier,
        int numberOfResult,
        boolean wildcardExpansion) {
        this.tenantId = tenantId;
        this.query = query;
        this.folderGuids = folderIds;
        this.userGuids = userIds;
        this.querier = querier;
        this.numberOfResult = numberOfResult;
        this.wildcardExpansion = wildcardExpansion;
    }
}
