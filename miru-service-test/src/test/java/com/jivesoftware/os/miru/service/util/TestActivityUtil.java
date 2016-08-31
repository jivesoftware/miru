package com.jivesoftware.os.miru.service.util;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.RandomStringUtils;

import static org.mockito.Mockito.mock;

public class TestActivityUtil {

    private static final AtomicInteger index = new AtomicInteger(0);
    private static final AtomicLong time = new AtomicLong(0);

    private static final Random random = new Random();

    public static MiruPartitionedActivity mockPartitionedActivity(int partitionId, byte[] streamId, Optional<MiruReadEvent> miruReadEvent,
        Optional<Integer> type) {
        int typeValue = type.or(random.nextInt(4));
        switch (typeValue) {
            case 0:
                return new MiruPartitionedActivityFactory().read(1, MiruPartitionId.of(partitionId), index.incrementAndGet(),
                    miruReadEvent.or(mockReadEvent(streamId)));
            case 1:
                return new MiruPartitionedActivityFactory().unread(1, MiruPartitionId.of(partitionId), index.incrementAndGet(),
                    miruReadEvent.or(mockReadEvent(streamId)));
            case 2:
                return new MiruPartitionedActivityFactory().allread(1, MiruPartitionId.of(partitionId), index.incrementAndGet(),
                    miruReadEvent.or(mockReadEvent(streamId)));
            default:
                return new MiruPartitionedActivityFactory().activity(1, MiruPartitionId.of(partitionId), index.incrementAndGet(), mockActivity());
        }
    }

    private static MiruReadEvent mockReadEvent(byte[] streamId) {
        return new MiruReadEvent("tenant1".getBytes(Charsets.UTF_8), time.incrementAndGet(), streamId, mock(MiruFilter.class));
    }

    private static MiruActivity mockActivity() {
        String[] authz = { RandomStringUtils.randomAlphanumeric(10) };
        return new MiruActivity.Builder(new MiruTenantId("tenant1".getBytes(Charsets.UTF_8)), time.incrementAndGet(), 0, false, authz)
            .putFieldValue("field1", "value1")
            .build();
    }

    private TestActivityUtil() {
    }

}
