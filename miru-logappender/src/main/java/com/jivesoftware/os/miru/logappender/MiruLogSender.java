package com.jivesoftware.os.miru.logappender;

import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface MiruLogSender {
    void send(List<MiruLogEvent> events) throws Exception;
}
