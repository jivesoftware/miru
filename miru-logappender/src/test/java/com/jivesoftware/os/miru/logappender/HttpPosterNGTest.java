package com.jivesoftware.os.miru.logappender;

import java.util.Arrays;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class HttpPosterNGTest {

    public HttpPosterNGTest() {
    }

    @Test(enabled = false)
    public void testSend() throws Exception {

        HttpPoster httpPoster = new HttpPoster("soa-prime-data7.phx1.jivehosted.com", 10000, 30000);

        httpPoster.send(Arrays.asList(
            new MiruLogEvent(null, "dev", "foo", "bar", "baz", "1", "INFO", "main", "logger", "meth", "line", "hi", "time", null, null)));
        httpPoster.send(Arrays.asList(
            new MiruLogEvent(null, "dev", "foo", "bar", "baz", "1", "INFO", "main", "logger", "meth", "line", "hi", "time", null, null)));
    }

}
