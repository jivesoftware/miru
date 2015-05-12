package com.jivesoftware.os.miru.plugin.solution;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author jonathan.colt
 */
public interface MiruSolutionMarshaller<Q, A, R> {

    MiruRequestAndReport<Q, R> requestAndReportFromBytes(byte[] requestBytes) throws IOException;

    byte[] requestAndReportToBytes(MiruRequestAndReport<Q, R> requestAndReport) throws IOException;

    MiruPartitionResponse<A> responseFromStream(InputStream is) throws IOException;

    void responseToStream(MiruPartitionResponse<A> response, OutputStream os) throws IOException;
}
