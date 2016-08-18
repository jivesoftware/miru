package com.jivesoftware.os.miru.service.index.lab;

import java.nio.ByteBuffer;

/**
 *
 */
public class LabKeyBytes {

    public final int key;
    public final ByteBuffer byteBuffer;

    public LabKeyBytes(int key, ByteBuffer byteBuffer) {
        this.key = key;
        this.byteBuffer = byteBuffer;
    }
}
