package com.jivesoftware.os.miru.service.index.lab;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class ReusableByteBufferDataInput implements DataInput {

    private ByteBuffer buf;

    public ReusableByteBufferDataInput() {
    }

    public void setBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        buf.get(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        buf.get(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int skippable = Math.min(n, buf.remaining());
        buf.position(buf.position() + skippable);
        return skippable;
    }

    @Override
    public boolean readBoolean() throws IOException {
        int ch = buf.get() & 0xFF;
        return (ch != 0);
    }

    @Override
    public byte readByte() throws IOException {
        return buf.get();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return buf.get() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        return buf.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return buf.getShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException {
        return buf.getChar();
    }

    @Override
    public int readInt() throws IOException {
        return buf.getInt();
    }

    @Override
    public long readLong() throws IOException {
        return buf.getLong();
    }

    @Override
    public float readFloat() throws IOException {
        return buf.getFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return buf.getDouble();
    }

    @Override
    public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException();
    }
}
