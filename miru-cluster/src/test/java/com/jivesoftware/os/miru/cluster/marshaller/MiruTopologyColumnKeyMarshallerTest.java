package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruTopologyColumnKeyMarshallerTest {

    @Test
    public void testFromBytes() throws Exception {
        MiruTopologyColumnKeyMarshaller marshaller = new MiruTopologyColumnKeyMarshaller();
        MiruTopologyColumnKey expected = new MiruTopologyColumnKey(MiruPartitionId.of(123), new MiruHost("a", 456));
        MiruTopologyColumnKey actual = marshaller.fromBytes(marshaller.toBytes(expected));
        assertEquals(actual.partitionId, expected.partitionId);
        assertEquals(actual.host, expected.host);
    }

    @Test
    public void testFromLexBytes() throws Exception {
        MiruTopologyColumnKeyMarshaller marshaller = new MiruTopologyColumnKeyMarshaller();
        MiruTopologyColumnKey expected = new MiruTopologyColumnKey(MiruPartitionId.of(123), new MiruHost("a", 456));
        MiruTopologyColumnKey actual = marshaller.fromLexBytes(marshaller.toLexBytes(expected));
        assertEquals(actual.partitionId, expected.partitionId);
        assertEquals(actual.host, expected.host);
    }
}
