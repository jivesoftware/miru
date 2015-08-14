package com.jivesoftware.os.miru.logappender;

/**
 *
 * @author jonathan.colt
 */
public interface MiruLogSenderProvider {

    MiruLogSender[] getLogSenders();
}
