package com.jivesoftware.os.miru.logappender;

public class MiruLogEvent {

    public String datacenter;
    public String cluster;
    public String host;
    public String service;
    public String instance;
    public String version;
    public String level;
    public String threadName;
    public String loggerName;
    public String methodName;
    public String lineNumber;
    public String message;
    public String timestamp;
    public String exceptionClass;
    public String[] thrownStackTrace;

    public MiruLogEvent() {
    }

    public MiruLogEvent(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version,
        String level,
        String threadName,
        String loggerName,
        String methodName,
        String lineNumber,
        String message,
        String timestamp,
        String exceptionClass,
        String[] thrownStackTrace) {
        this.datacenter = datacenter;
        this.cluster = cluster;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.level = level;
        this.threadName = threadName;
        this.loggerName = loggerName;
        this.methodName = methodName;
        this.lineNumber = lineNumber;
        this.message = message;
        this.timestamp = timestamp;
        this.exceptionClass = exceptionClass;
        this.thrownStackTrace = thrownStackTrace;
    }

}
