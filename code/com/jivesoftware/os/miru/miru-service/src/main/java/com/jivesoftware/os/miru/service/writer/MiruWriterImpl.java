package com.jivesoftware.os.miru.service.writer;

import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.service.MiruService;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 * @author jonathan
 */
@Singleton
public class MiruWriterImpl implements MiruWriter {

    private final MiruService miruService;

    @Inject
    public MiruWriterImpl(MiruService miruService) {
        this.miruService = miruService;
    }

    @Override
    public void writeToIndex(List<MiruPartitionedActivity> activities) throws Exception {
        miruService.writeToIndex(activities);
    }

    @Override
    public void writeToWAL(List<MiruPartitionedActivity> activities) throws Exception {
        miruService.writeWAL(activities);
    }
}
