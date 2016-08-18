package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.api.KeyValueRawhide;
import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;
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

        byte[] rawKey = { 0, 1, 2 };
        long rawTimestamp = 123L;
        byte[] rawPayload = { 3, 4, 5 };
        byte[] mergedPayload = new byte[rawPayload.length + 4];
        System.arraycopy(rawPayload, 0, mergedPayload, 0, rawPayload.length);
        UIO.intBytes((int) rawTimestamp, mergedPayload, mergedPayload.length - 4);
        byte[] rawEntry = keyValueRawhide.toRawEntry(rawKey, 0, false, 0L, mergedPayload);

        Assert.assertEquals(lastIdKeyValueRawhide.timestamp(NO_OP, NO_OP, ByteBuffer.wrap(rawEntry)), rawTimestamp);

        byte[] lastIdRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, rawTimestamp, false, 0L, rawPayload);
        Assert.assertEquals(lastIdRawEntry, rawEntry);
        Assert.assertEquals(lastIdKeyValueRawhide.timestamp(NO_OP, NO_OP, ByteBuffer.wrap(lastIdRawEntry)), rawTimestamp);

        lastIdKeyValueRawhide.streamRawEntry(0, NO_OP, NO_OP, ByteBuffer.wrap(rawEntry), (index, key, timestamp, tombstoned, version, payload) -> {
            key.clear();
            payload.clear();
            Assert.assertEquals(key, ByteBuffer.wrap(rawKey));
            Assert.assertEquals(timestamp, rawTimestamp);
            Assert.assertEquals(payload, ByteBuffer.wrap(rawPayload));
            return true;
        }, true);
    }

    @Test
    public void testMerge() throws Exception {
        LastIdKeyValueRawhide lastIdKeyValueRawhide = new LastIdKeyValueRawhide();

        byte[] rawKey = { 0, 1, 2 };
        byte[] rawPayload = { 3, 4, 5 };

        byte[] firstRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, 123L, false, 0L, rawPayload);
        byte[] secondRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, 124L, false, 0L, rawPayload);

        Assert.assertSame(lastIdKeyValueRawhide.merge(NO_OP, NO_OP, firstRawEntry, NO_OP, NO_OP, secondRawEntry, NO_OP, NO_OP), secondRawEntry);
        Assert.assertSame(lastIdKeyValueRawhide.merge(NO_OP, NO_OP, secondRawEntry, NO_OP, NO_OP, firstRawEntry, NO_OP, NO_OP), secondRawEntry);
    }
}