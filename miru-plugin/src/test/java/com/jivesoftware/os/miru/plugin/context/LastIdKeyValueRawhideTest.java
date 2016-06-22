package com.jivesoftware.os.miru.plugin.context;

import com.jivesoftware.os.lab.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class LastIdKeyValueRawhideTest {

    @Test
    public void testReverseCompatibility() throws Exception {
        KeyValueRawhide keyValueRawhide = new KeyValueRawhide();
        LastIdKeyValueRawhide lastIdKeyValueRawhide = new LastIdKeyValueRawhide();

        byte[] rawKey = { 0, 1, 2 };
        long rawTimestamp = 123L;
        byte[] rawPayload = { 3, 4, 5 };
        byte[] mergedPayload = new byte[rawPayload.length + 4];
        System.arraycopy(rawPayload, 0, mergedPayload, 0, rawPayload.length);
        UIO.intBytes((int) rawTimestamp, mergedPayload, mergedPayload.length - 4);
        byte[] rawEntry = keyValueRawhide.toRawEntry(rawKey, 0, false, 0L, mergedPayload);

        Assert.assertEquals(lastIdKeyValueRawhide.timestamp(rawEntry, 0, rawEntry.length), rawTimestamp);

        byte[] lastIdRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, rawTimestamp, false, 0L, rawPayload);
        Assert.assertEquals(lastIdRawEntry, rawEntry);
        Assert.assertEquals(lastIdKeyValueRawhide.timestamp(lastIdRawEntry, 0, lastIdRawEntry.length), rawTimestamp);

        lastIdKeyValueRawhide.streamRawEntry((index, key, timestamp, tombstoned, version, payload) -> {
            Assert.assertEquals(key, rawKey);
            Assert.assertEquals(timestamp, rawTimestamp);
            Assert.assertEquals(payload, rawPayload);
            return true;
        }, 0, rawEntry, 0);
    }

    @Test
    public void testMerge() throws Exception {
        LastIdKeyValueRawhide lastIdKeyValueRawhide = new LastIdKeyValueRawhide();

        byte[] rawKey = { 0, 1, 2 };
        byte[] rawPayload = { 3, 4, 5 };

        byte[] firstRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, 123L, false, 0L, rawPayload);
        byte[] secondRawEntry = lastIdKeyValueRawhide.toRawEntry(rawKey, 124L, false, 0L, rawPayload);

        Assert.assertSame(lastIdKeyValueRawhide.merge(firstRawEntry, secondRawEntry), secondRawEntry);
        Assert.assertSame(lastIdKeyValueRawhide.merge(secondRawEntry, firstRawEntry), secondRawEntry);
    }
}