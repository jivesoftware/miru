package com.jivesoftware.os.miru.syslog.deployable;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class SyslogEventParseTest {

    @Test
    public void testParseFormat1() throws Exception {

        String message = "Nov 21 17:32:09 ip-10-126-5-158 ec2net: [rewrite_aliases] Rewriting aliases of eth0";
        String host = "1.2.3.4";
        SyslogEvent syslogEvent = new SyslogEvent()
                .setMessage(message)
                .setAddress(new InetSocketAddress(InetAddress.getByName(host), 1))
                .build();

        Assert.assertNotNull(syslogEvent.miruLogEvent);
        Assert.assertEquals(syslogEvent.miruLogEvent.host, "ip-10-126-5-158");
        Assert.assertEquals(syslogEvent.miruLogEvent.service, "ec2net");
        Assert.assertEquals(syslogEvent.miruLogEvent.message, "[rewrite_aliases] Rewriting aliases of eth0");

    }

}
