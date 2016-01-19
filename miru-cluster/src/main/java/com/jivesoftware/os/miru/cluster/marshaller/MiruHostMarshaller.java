package com.jivesoftware.os.miru.cluster.marshaller;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;

/**
 *
 */
public class MiruHostMarshaller implements TypeMarshaller<MiruHost> {

    @Override
    public MiruHost fromBytes(byte[] bytes) throws Exception {
        return fromLexBytes(bytes);
    }

    @Override
    public byte[] toBytes(MiruHost miruHost) throws Exception {
        return toLexBytes(miruHost);
    }

    @Override
    public MiruHost fromLexBytes(byte[] bytes) throws Exception {
        Preconditions.checkNotNull(bytes);
        return new MiruHost(new String(bytes, Charsets.UTF_8));
    }

    @Override
    public byte[] toLexBytes(MiruHost miruHost) throws Exception {
        Preconditions.checkNotNull(miruHost);
        return miruHost.getLogicalName().getBytes(Charsets.UTF_8);
    }
}
