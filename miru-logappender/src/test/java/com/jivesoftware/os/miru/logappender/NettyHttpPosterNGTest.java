package com.jivesoftware.os.miru.logappender;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class NettyHttpPosterNGTest {

    public NettyHttpPosterNGTest() {
    }

    @Test(enabled = false)
    public void testSend() throws Exception {

        NettyHttpPoster httpPoster = new NettyHttpPoster("10.126.5.155", 10_004, 60_000);

        httpPoster.send(Collections.singletonList(
                new MiruLogEvent(null, "dev", "foo", "bar", "baz", "1", "INFO", "main", "logger", "meth", "line", "hi", String.valueOf(System.currentTimeMillis()) + 1, null, null)));
        httpPoster.send(Collections.singletonList(
                new MiruLogEvent(null, "dev", "foo", "bar", "baz", "2", "INFO", "main", "logger", "meth", "line", "hi", String.valueOf(System.currentTimeMillis()) + 2, null, null)));
        httpPoster.send(Arrays.asList(
                new MiruLogEvent(null, "dev", "foo", "bar", "baz", "3", "INFO", "main", "logger", "meth", "line", "hi", String.valueOf(System.currentTimeMillis()) + 3, null, null),
                new MiruLogEvent(null, "dev", "foo", "bar", "baz", "4", "INFO", "main", "logger", "meth", "line", "hi", String.valueOf(System.currentTimeMillis()) + 4, null, null)));

        httpPoster.close();
    }

}
