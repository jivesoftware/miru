package com.jivesoftware.os.miru.service.index;

import java.util.concurrent.ExecutorService;

/**
 *
 */
public interface Compactable {

    void compact(ExecutorService executorService) throws Exception;
}
