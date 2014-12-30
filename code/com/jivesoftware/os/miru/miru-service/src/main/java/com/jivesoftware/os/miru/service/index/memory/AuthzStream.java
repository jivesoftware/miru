package com.jivesoftware.os.miru.service.index.memory;

import java.io.IOException;

/**
 *
 */
public interface AuthzStream<BM> {
    boolean stream(String authz, BM bitmap) throws IOException;
}
