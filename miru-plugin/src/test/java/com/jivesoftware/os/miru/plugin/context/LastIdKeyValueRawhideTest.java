package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.api.rawhide.KeyValueRawhide;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.jivesoftware.os.lab.api.FormatTransformer.NO_OP;

/**
 *
 */
public class LastIdKeyValueRawhideTest {

    @Test
    public void testReverseCompatibility() throws Exception {
        KeyValueRawhide keyValueRawhide = KeyValueRawhide.SINGLETON;
        LastIdKeyValueRawhide lastIdKeyValueRawhide = new LastIdKeyValueRawhide();

        byte[] rawKey = {0, 1, 2};
        long rawTimestamp = 123L;
        byte[] rawPayload = {3, 4, 5};
        byte[] mergedPayload = new byte[rawPayload.length + 4];
        System.arraycopy(rawPayload, 0, mergedPayload, 0, rawPayload.length);
        UIO.intBytes((int) rawTimestamp, mergedPayload, mergedPayload.length - 4);
        BolBuffer rawEntry = keyValueRawhide.toRawEntry(rawKey, 0, false, 0L, mergedPayload, new BolBuffer());

        Assert.assertEquals(lastIdKeyValueRawhide.timestamp(NO_OP, NO_OP, rawEntry), rawTimestamp);

        BolBuffer lastIdRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, rawTimestamp, false, 0L, rawPayload, new BolBuffer());

        Assert.assertEquals(lastIdRawEntry.copy(),
            rawEntry.copy(),
            Arrays.toString(lastIdRawEntry.copy()) + " vs " + Arrays.toString(rawEntry.copy()));

        Assert.assertEquals(lastIdKeyValueRawhide.timestamp(NO_OP, NO_OP, lastIdRawEntry), rawTimestamp);

        lastIdKeyValueRawhide.streamRawEntry(0, NO_OP, NO_OP, new BolBuffer(rawEntry.copy()), (index, key, timestamp, tombstoned, version, payload) -> {
            Assert.assertEquals(key.copy(), rawKey);
            Assert.assertEquals(timestamp, rawTimestamp);
            Assert.assertEquals(payload.copy(), rawPayload);
            return true;
        }, true);
    }

    @Test
    public void testMerge() throws Exception {
        LastIdKeyValueRawhide lastIdKeyValueRawhide = new LastIdKeyValueRawhide();

        byte[] rawKey = {0, 1, 2};
        byte[] rawPayload = {3, 4, 5};

        BolBuffer firstRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, 123L, false, 0L, rawPayload, new BolBuffer());
        BolBuffer secondRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, 124L, false, 0L, rawPayload, new BolBuffer());

        Assert.assertSame(lastIdKeyValueRawhide.merge(NO_OP, NO_OP, firstRawEntry, NO_OP, NO_OP, secondRawEntry, NO_OP, NO_OP), secondRawEntry);
        Assert.assertSame(lastIdKeyValueRawhide.merge(NO_OP, NO_OP, secondRawEntry, NO_OP, NO_OP, firstRawEntry, NO_OP, NO_OP), secondRawEntry);
    }
}
