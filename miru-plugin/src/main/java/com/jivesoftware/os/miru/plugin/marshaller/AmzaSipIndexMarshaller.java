package com.jivesoftware.os.miru.plugin.marshaller;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.context.MiruContextConstants;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndexMarshaller;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class AmzaSipIndexMarshaller implements MiruSipIndexMarshaller<AmzaSipCursor> {

    @Override
    public byte[] getSipIndexKey() {
        return MiruContextConstants.GENERIC_FILER_AMZA_SIP_INDEX_KEY;
    }

    @Override
    public long expectedCapacity(AmzaSipCursor cursor) {
        int length = 4;
        for (NamedCursor namedCursor : cursor.cursors) {
            byte[] nameBytes = namedCursor.name.getBytes(Charsets.UTF_8);
            length += 8 + 4 + nameBytes.length;
        }
        length += 1;
        return length;
    }

    @Override
    public AmzaSipCursor fromFiler(Filer filer, StackBuffer stackBuffer) throws IOException {
        byte[] bytes = FilerIO.readByteArray(filer, "bytes", stackBuffer);
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
        boolean endOfStream = (filer.length() - filer.getFilePointer()) > 0 && (FilerIO.readByte(filer, "endOfStream") == 1);
        return new AmzaSipCursor(namedCursors, endOfStream);
    }

    @Override
    public void toFiler(Filer filer, AmzaSipCursor cursor, StackBuffer stackBuffer) throws IOException {

        int length = (int) expectedCapacity(cursor);

        ByteBuffer buf = ByteBuffer.allocate(length);

        buf.putInt(cursor.cursors.size());
        for (NamedCursor namedCursor : cursor.cursors) {
            byte[] nameBytes = namedCursor.name.getBytes(Charsets.UTF_8);
            buf.putLong(namedCursor.id);
            buf.putInt(nameBytes.length);
            buf.put(nameBytes);
        }

        FilerIO.writeByteArray(filer, buf.array(), "bytes", stackBuffer);
        FilerIO.writeByte(filer, cursor.endOfStream ? (byte) 1 : (byte) 0, "endOfStream");
    }
}
