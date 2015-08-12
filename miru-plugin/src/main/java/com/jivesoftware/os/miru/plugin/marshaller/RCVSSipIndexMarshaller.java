package com.jivesoftware.os.miru.plugin.marshaller;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.context.MiruContextConstants;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndexMarshaller;
import java.io.IOException;

/**
 *
 */
public class RCVSSipIndexMarshaller implements MiruSipIndexMarshaller<RCVSSipCursor> {

    @Override
    public byte[] getSipIndexKey() {
        return MiruContextConstants.GENERIC_FILER_RCVS_SIP_INDEX_KEY;
    }

    @Override
    public long expectedCapacity(RCVSSipCursor sip) {
        return 8 + 1 + 8 + 8 + 1 + 8;
    }

    @Override
    public RCVSSipCursor fromFiler(Filer filer) throws IOException {
        long marker = FilerIO.readLong(filer, "marker");
        if (marker == Long.MIN_VALUE) {
            byte sort = FilerIO.readByte(filer, "sort");
            long clockTimestamp = FilerIO.readLong(filer, "clockTimestamp");
            long activityTimestamp = FilerIO.readLong(filer, "activityTimestamp");
            boolean endOfStream = FilerIO.readByte(filer, "endOfStream") == (byte) 1;
            return new RCVSSipCursor(sort, clockTimestamp, activityTimestamp, endOfStream);
        } else {
            // legacy, marker becomes clockTimestamp
            long activityTimestamp = FilerIO.readLong(filer, "activityTimestamp");
            return new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), marker, activityTimestamp, false);
        }
    }

    @Override
    public void toFiler(Filer filer, RCVSSipCursor cursor) throws IOException {
        FilerIO.writeLong(filer, Long.MIN_VALUE, "marker");
        FilerIO.writeByte(filer, cursor.sort, "sort");
        FilerIO.writeLong(filer, cursor.clockTimestamp, "clockTimestamp");
        FilerIO.writeLong(filer, cursor.activityTimestamp, "activityTimestamp");
        FilerIO.writeByte(filer, cursor.endOfStream ? (byte) 1 : (byte) 0, "endOfStream");
    }
}
