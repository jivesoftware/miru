package com.jivesoftware.os.miru.api;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import java.util.List;

/**
 *
 * @author jonathan
 */
public interface MiruWriter {

    public static final String WRITER_SERVICE_ENDPOINT_PREFIX = "/miru/writer";
    public static final String ADD_ACTIVITIES = "/add/activities";

    void writeToIndex(List<MiruPartitionedActivity> activities) throws Exception;
}
