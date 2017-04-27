package com.jivesoftware.os.miru.siphon.deployable.region;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jonathan.colt on 12/1/16.
 */
public interface WikiQuerier {


    Found queryContent(WikiMiruPluginRegionInput input,
        Set<String> uniqueFolders, Set<String> uniqueUsers, Map<String, Integer> foldersIndex, Map<String, Integer> usersIndex, List<String> contentKeys,
        List<String> folderKeys, List<String> userKeys) throws Exception;

    Found queryUsers(WikiMiruPluginRegionInput input) throws Exception;

    Found queryFolders(WikiMiruPluginRegionInput input) throws Exception;


    class Found {
        public final long elapse;
        public final long totalPossible;
        public final List<Result> results;


        public Found(long elapse, long totalPossible, List<Result> results) {
            this.elapse = elapse;
            this.totalPossible = totalPossible;
            this.results = results;
        }
    }

    class Result {
        public final String userGuid;
        public final String folderGuid;
        public final String guid;
        public final String type;


        public Result(String userGuid, String folderGuid, String guid, String type) {
            this.userGuid = userGuid;
            this.folderGuid = folderGuid;
            this.guid = guid;
            this.type = type;
        }
    }
}
