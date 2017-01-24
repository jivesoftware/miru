package com.jivesoftware.os.miru.service.index;

import java.util.concurrent.ExecutorService;

/**
 *
 */
public interface Commitable {

    void commit() throws Exception;
}
