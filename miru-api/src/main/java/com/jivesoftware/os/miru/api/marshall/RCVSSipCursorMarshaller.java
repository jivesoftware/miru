package com.jivesoftware.os.miru.api.marshall;

import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;

/**
 *
 */
public class RCVSSipCursorMarshaller implements TypeMarshaller<RCVSSipCursor> {

    @Override
    public RCVSSipCursor fromBytes(byte[] bytes) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return new RCVSSipCursor(buf.get(), buf.getLong(), buf.getLong(), buf.get() == (byte) 1);
    }

    @Override
    public byte[] toBytes(RCVSSipCursor cursor) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(18);

        buf.put(cursor.sort);
        buf.putLong(cursor.clockTimestamp);
        buf.putLong(cursor.activityTimestamp);
        buf.put(cursor.endOfStream ? (byte) 1 : (byte) 0);

        return buf.array();
    }

    @Override
    public RCVSSipCursor fromLexBytes(byte[] bytes) throws Exception {
        return fromBytes(bytes);
    }

    @Override
    public byte[] toLexBytes(RCVSSipCursor cursor) throws Exception {
        return toBytes(cursor);
    }
}
