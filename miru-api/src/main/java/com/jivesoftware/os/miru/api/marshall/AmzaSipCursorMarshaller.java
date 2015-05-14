package com.jivesoftware.os.miru.api.marshall;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class AmzaSipCursorMarshaller implements TypeMarshaller<AmzaSipCursor> {

    @Override
    public AmzaSipCursor fromBytes(byte[] bytes) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        List<NamedCursor> namedCursors = Lists.newArrayList();
        int numCursors = buf.getInt();
        for (int i = 0; i < numCursors; i++) {
            long id = buf.getLong();
            int nameLength = buf.getInt();
            byte[] nameBytes = new byte[nameLength];
            buf.get(nameBytes);
            namedCursors.add(new NamedCursor(new String(nameBytes, Charsets.UTF_8), id));
        }
        return new AmzaSipCursor(namedCursors);
    }

    @Override
    public byte[] toBytes(AmzaSipCursor cursor) throws Exception {

        int length = 4;
        for (NamedCursor namedCursor : cursor.cursors) {
            byte[] nameBytes = namedCursor.name.getBytes(Charsets.UTF_8);
            length += 8 + 4 + nameBytes.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(length);

        buf.putInt(cursor.cursors.size());
        for (NamedCursor namedCursor : cursor.cursors) {
            byte[] nameBytes = namedCursor.name.getBytes(Charsets.UTF_8);
            buf.putLong(namedCursor.id);
            buf.putInt(nameBytes.length);
            buf.put(nameBytes);
        }

        return buf.array();
    }

    @Override
    public AmzaSipCursor fromLexBytes(byte[] bytes) throws Exception {
        return fromBytes(bytes);
    }

    @Override
    public byte[] toLexBytes(AmzaSipCursor cursor) throws Exception {
        return toBytes(cursor);
    }
}
