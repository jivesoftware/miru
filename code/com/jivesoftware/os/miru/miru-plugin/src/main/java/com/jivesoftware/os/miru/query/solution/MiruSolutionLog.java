package com.jivesoftware.os.miru.query.solution;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruSolutionLog {

    private final boolean enabled;
    private final List<String> log = new ArrayList<>();

    public MiruSolutionLog(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void log(String message) {
        if (enabled) {
            log.add(message);
        }
    }

    public List<String> asList() {
        return log;
    }

    public void clear() {
        log.clear();
    }
}
