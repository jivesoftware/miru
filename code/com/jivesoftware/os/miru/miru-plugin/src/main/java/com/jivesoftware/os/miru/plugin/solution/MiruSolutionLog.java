package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.jive.utils.logger.MessageFormatter;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruSolutionLog {

    private static final MetricLogger METRIC_LOGGER = MetricLoggerFactory.getLogger();

    private final boolean enabled;
    private final List<String> log = new ArrayList<>();

    public MiruSolutionLog(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void log(String message) {
        METRIC_LOGGER.debug(message);
        if (enabled) {
            log.add(message);
        }
    }

    public void log(String message, Object... args) {
        METRIC_LOGGER.debug(message, args);
        if (enabled) {
            log.add(MessageFormatter.format(message, args));
        }
    }

    public List<String> asList() {
        return log;
    }

    public void clear() {
        log.clear();
    }
}
