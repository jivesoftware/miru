package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruTopologyColumnValueMarshallerTest {

    @Test
    public void testFromLexBytes() throws Exception {
        MiruTopologyColumnValueMarshaller marshaller = new MiruTopologyColumnValueMarshaller();
        MiruTopologyColumnValue expected = new MiruTopologyColumnValue(MiruPartitionState.online, MiruBackingStorage.hybrid, System.currentTimeMillis());
        MiruTopologyColumnValue actual = marshaller.fromLexBytes(marshaller.toLexBytes(expected));
        assertEquals(actual.state, expected.state);
        assertEquals(actual.storage, expected.storage);
        assertEquals(actual.lastActiveTimestamp, expected.lastActiveTimestamp);
    }
}
