package com.jivesoftware.os.wiki.miru.deployable.region;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public class WikiMiruPluginRegionInput {

    final String tenantId;
    final String query;
    final String folderGuids;
    final String userGuids;
    final String querier;

    public WikiMiruPluginRegionInput(String tenantId, String query, String folderIds, String userIds, String querier) {
        this.tenantId = tenantId;
        this.query = query;
        this.folderGuids = folderIds;
        this.userGuids = userIds;
        this.querier = querier;
    }
}
