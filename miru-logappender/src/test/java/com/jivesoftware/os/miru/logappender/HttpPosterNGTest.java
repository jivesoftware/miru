package com.jivesoftware.os.miru.logappender;

import java.util.Collections;

import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class HttpPosterNGTest {

    public HttpPosterNGTest() {
    }

    @Test(enabled = false)
    public void testSend() throws Exception {

        HttpPoster httpPoster = new HttpPoster("soa-prime-data7.phx1.jivehosted.com", 10_000, 30_000);

        httpPoster.send(Collections.singletonList(
                new MiruLogEvent(null, "dev", "foo", "bar", "baz", "1", "INFO", "main", "logger", "meth", "line", "hi", String.valueOf(System.currentTimeMillis()) + 1, null, null)));
        httpPoster.send(Collections.singletonList(
                new MiruLogEvent(null, "dev", "foo", "bar", "baz", "1", "INFO", "main", "logger", "meth", "line", "hi", String.valueOf(System.currentTimeMillis()) + 2, null, null)));
    }

}
