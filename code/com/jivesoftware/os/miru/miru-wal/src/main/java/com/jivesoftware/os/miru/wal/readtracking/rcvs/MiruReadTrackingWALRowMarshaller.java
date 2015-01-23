package com.jivesoftware.os.miru.wal.readtracking.rcvs;

import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;

public class MiruReadTrackingWALRowMarshaller implements TypeMarshaller<MiruReadTrackingWALRow> {

    @Override
    public MiruReadTrackingWALRow fromBytes(byte[] bytes) throws Exception {
        return new MiruReadTrackingWALRow(new MiruStreamId(bytes));
    }

    @Override
    public byte[] toBytes(MiruReadTrackingWALRow miruReadTrackingWALRow) throws Exception {
        return miruReadTrackingWALRow.getStreamId().getBytes();
    }

    @Override
    public MiruReadTrackingWALRow fromLexBytes(byte[] bytes) throws Exception {
        return fromBytes(bytes);
    }

    @Override
    public byte[] toLexBytes(MiruReadTrackingWALRow miruReadTrackingWALRow) throws Exception {
        return toBytes(miruReadTrackingWALRow);
    }
}