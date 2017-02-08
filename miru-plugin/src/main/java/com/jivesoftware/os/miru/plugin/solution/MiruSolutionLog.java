package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.mlogger.core.MessageFormatter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;

import static com.sun.tools.doclint.Entity.times;

/**
 * @author jonathan.colt
 */
public class MiruSolutionLog {

    private static final MetricLogger METRIC_LOGGER = MetricLoggerFactory.getLogger();

    private final MiruSolutionLogLevel level;
    private final List<String> log = new ArrayList<>();
    private volatile long lastLoggedTimestampMillis = -1;

    public MiruSolutionLog(MiruSolutionLogLevel level) {
        this.level = level;
    }

    public MiruSolutionLogLevel getLevel() {
        return level;
    }

    public boolean isLogLevelEnabled(MiruSolutionLogLevel checkLevel) {
        return level.ordinal() <= checkLevel.ordinal();
    }

    public void log(MiruSolutionLogLevel atLevel, String message) {
        if (isLogLevelEnabled(atLevel)) {
            long time = System.currentTimeMillis();
            long delta = lastLoggedTimestampMillis == -1 ? 0L : time - lastLoggedTimestampMillis;
            lastLoggedTimestampMillis = time;
            log.add(time + " (" + delta + ") " + message);
            METRIC_LOGGER.debug(message);
        } else {
            METRIC_LOGGER.trace(message);
        }
    }

    public void log(MiruSolutionLogLevel atLevel, String message, Object... args) {
        if (isLogLevelEnabled(atLevel)) {
            long time = System.currentTimeMillis();
            long delta = lastLoggedTimestampMillis == -1 ? 0L : time - lastLoggedTimestampMillis;
            lastLoggedTimestampMillis = time;
            log.add(time + " (" + delta + ") " + MessageFormatter.format(message, args));
            METRIC_LOGGER.debug(message, args);
        } else {
            METRIC_LOGGER.trace(message, args);
        }
    }

    public List<String> asList() {
        return log;
    }

    public void clear() {
        log.clear();
    }

    public void append(MiruSolutionLog append) {
        log.addAll(append.asList());
    }
}
