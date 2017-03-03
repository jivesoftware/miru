package com.jivesoftware.os.miru.syslog.deployable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SyslogEventDateTest {

    @Test
    public void testShortDateFormat() throws Exception {

        // Nov 15 23:23:49

        String currentYear = Integer.toString(Calendar.getInstance().get(Calendar.YEAR));
        String sDate = "Nov 15 23:23:49";
        String originalDate = sDate + " " + currentYear;
        DateFormat dateFormat = new SimpleDateFormat("MMM dd HH:mm:ss yyyy");
        long timestamp = dateFormat.parse(originalDate).getTime();
        Assert.assertTrue(timestamp > 0);

    }

    @Test
    public void testShorterDateFormat() throws Exception {

        // Nov 5 23:23:49

        String currentYear = Integer.toString(Calendar.getInstance().get(Calendar.YEAR));
        String sDate = "Nov 5 23:23:49";
        String originalDate = sDate + " " + currentYear;
        DateFormat dateFormat = new SimpleDateFormat("MMM d HH:mm:ss yyyy");
        long timestamp = dateFormat.parse(originalDate).getTime();
        Assert.assertTrue(timestamp > 0);

    }

    @Test
    public void testISODateFormat() throws Exception {

        // 2016-11-17T16:51:26.767584-08:00

        String originalDate = "2016-11-17T16:51:26.767584-08:00";
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");
        long timestamp = dateFormat.parse(originalDate).getTime();
        Assert.assertTrue(timestamp > 0);

    }

}
