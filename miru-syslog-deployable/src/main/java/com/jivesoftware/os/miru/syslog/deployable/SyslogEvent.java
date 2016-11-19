package com.jivesoftware.os.miru.syslog.deployable;

import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;

class SyslogEvent {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private String rawMessage;
    private SocketAddress address;

    MiruLogEvent miruLogEvent;

    SyslogEvent setMessage(String message) {
        this.rawMessage = message;
        return this;
    }

    SyslogEvent setAddress(InetSocketAddress address) {
        this.address = address;
        return this;
    }

    SyslogEvent build() {
        /*
        parse the simplest known format:
        date host app[pid]: message

        i.e.
        Nov 15 21:24:55 localhost kernel: x86/fpu: Supporting XSAVE feature 0x001: 'x87 floating point registers'
        Nov 17 23:23:49 e1 etcd2[889]: compacted raft log at 95010
        Nov 17 23:24:10 e1 systemd[1]: Time has been changed
        2016-11-17T16:51:26.767584-08:00 soa-prime-data1 /usr/sbin/gmetad[2263]: data_thread() got no answer from any [my cluster] datasource
        */

        miruLogEvent = parse(rawMessage, address.toString());
        return this;
    }

    private static MiruLogEvent parse(String message, String address) {
        MiruLogEvent res = new MiruLogEvent();

        // date
        {
            int dateLength = 16;
            String sFormat = "MMM dd HH:mm:ss yyyy";
            if (message.length() > dateLength) {
                if (message.charAt(5) == ' ') {
                    dateLength = 15;
                    sFormat = "MMM d HH:mm:ss yyyy";
                }

                String currentYear = Integer.toString(Calendar.getInstance().get(Calendar.YEAR));
                String originalDate = message.substring(0, dateLength - 1) + " " + currentYear;
                DateFormat dateFormat = new SimpleDateFormat(sFormat);

                try {
                    res.timestamp = String.valueOf(dateFormat.parse(originalDate).getTime());
                    message = message.substring(dateLength);
                } catch (ParseException e) {
                    LOG.debug("Error occurred parsing date {}", originalDate);
                }
            }
        }
        if (res.timestamp == null) {
            int firstSpace = message.indexOf(' ');
            String sFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX";
            if (firstSpace > -1) {
                String originalDate = message.substring(0, firstSpace);
                DateFormat dateFormat = new SimpleDateFormat(sFormat);

                try {
                    res.timestamp = String.valueOf(dateFormat.parse(originalDate).getTime());
                    message = message.substring(firstSpace + 1);
                } catch (ParseException e) {
                    LOG.debug("Error occurred parsing date {}", originalDate);
                }
            }
        }
        if (res.timestamp == null) {
            LOG.error("Error occurred parsing date {}", message);
            return null;
        }

        // override host
        {
            int firstSpace = message.indexOf(' ');
            if (firstSpace < 0) {
                res.host = address;
            } else {
                res.host = message.substring(0, firstSpace).trim();
                message = message.substring(firstSpace + 1);
            }
        }

        // application:
        // application[pid]:
        {
            int firstSpace = message.indexOf(' ');
            if (firstSpace > -1) {
                String appPid = message.substring(0, firstSpace).trim();
                if (appPid.endsWith(":")) {
                    appPid = appPid.substring(0, appPid.length() - 1);
                }

                int leftBrace = appPid.indexOf('[');
                int rightBrace = appPid.indexOf(']');
                if (leftBrace > 0 && rightBrace > 0) {
                    res.service = message.substring(0, leftBrace).trim();
                    res.instance = message.substring(leftBrace + 1, rightBrace).trim();
                }

                message = message.substring(firstSpace + 1);
            }
        }

        res.level = Level.INFO.toString();
        res.message = message;

        return res;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SyslogEvent{");

        if (miruLogEvent != null) {
            sb.append("miruLogEvent.host=");
            sb.append(miruLogEvent.host);
            sb.append(", miruLogEvent.service=");
            sb.append(miruLogEvent.service);
            sb.append(", miruLogEvent.instance=");
            sb.append(miruLogEvent.instance);
            sb.append(", miruLogEvent.message=");
            sb.append(miruLogEvent.message);
            sb.append(", miruLogEvent.timestamp=");
            sb.append(miruLogEvent.timestamp);
        }

        sb.append(", address=");
        sb.append(address);
        sb.append(", rawMessage='");
        sb.append(rawMessage);
        sb.append('\'');
        sb.append('}');

        return sb.toString();
    }

}
