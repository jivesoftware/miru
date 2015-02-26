package com.jivesoftware.os.miru.cluster.marshaller;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.marshall.MiruPartitionIdMarshaller;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruPartitionIdMarshallerTest {

    @Test
    public void testFromBytes() throws Exception {
        MiruPartitionIdMarshaller marshaller = new MiruPartitionIdMarshaller();
        MiruPartitionId expected = MiruPartitionId.of(123);
        assertEquals(marshaller.fromBytes(marshaller.toBytes(expected)), expected);
    }

    @Test
    public void testFromLexBytes() throws Exception {
        MiruPartitionIdMarshaller marshaller = new MiruPartitionIdMarshaller();
        MiruPartitionId expected = MiruPartitionId.of(123);
        assertEquals(marshaller.fromLexBytes(marshaller.toLexBytes(expected)), expected);
    }
}
