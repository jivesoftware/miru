package com.jivesoftware.os.miru.metric.sampler;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class JVMMetrics {

    public static final JVMMetrics INSTANCE = new JVMMetrics();
    private final static MetricLogger logger = MetricLoggerFactory.getLogger();
    private final List<GarbageCollectorMXBean> garbageCollectors;
    private final OperatingSystemMXBean osBean;
    private final ThreadMXBean threadBean;
    private final MemoryMXBean memoryBean;
    private final RuntimeMXBean runtimeBean;
    private final List<JVMStat> stats;

    private JVMMetrics() {

        garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
        osBean = ManagementFactory.getOperatingSystemMXBean();
        threadBean = ManagementFactory.getThreadMXBean();
        memoryBean = ManagementFactory.getMemoryMXBean();
        runtimeBean = ManagementFactory.getRuntimeMXBean();
        stats = new LinkedList<>();

        add(new JVMStat("jvm>startTime:millis") {

            @Override
            public long stat() {
                return runtimeBean.getStartTime();
            }
        });
        add(new JVMStat("jvm>upTime:millis") {

            @Override
            public long stat() {
                return runtimeBean.getUptime();
            }
        });
        add(new JVMStat("jvm>os>loadAverage") {

            @Override
            public long stat() {
                return (long) (osBean.getSystemLoadAverage() * 100);
            }
        });
        add(new JVMStat("jvm>threads>currentThreadCpuTime") {

            @Override
            public long stat() {
                return threadBean.getCurrentThreadCpuTime();
            }
        });
        add(new JVMStat("jvm>threads>currentThreadUserTime") {

            @Override
            public long stat() {
                return threadBean.getCurrentThreadUserTime();
            }
        });
        add(new JVMStat("jvm>threads>daemonThreadCount") {

            @Override
            public long stat() {
                return threadBean.getDaemonThreadCount();
            }
        });
        add(new JVMStat("jvm>threads>peakThreadCount") {

            @Override
            public long stat() {
                return threadBean.getPeakThreadCount();
            }
        });
        add(new JVMStat("jvm>threads>threadCount") {

            @Override
            public long stat() {
                return threadBean.getThreadCount();
            }
        });
        add(new JVMStat("jvm>threads>totalStartedThreadCount") {

            @Override
            public long stat() {
                return threadBean.getTotalStartedThreadCount();
            }
        });
        add(new JVMStat("jvm>memory>heap>commited:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getCommitted();
            }
        });
        add(new JVMStat("jvm>memory>heap>init:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getInit();
            }
        });
        add(new JVMStat("jvm>memory>heap>max:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getMax();
            }
        });
        add(new JVMStat("jvm>memory>heap>used:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getUsed();
            }
        });
        add(new JVMStat("jvm>memory>nonheap>commited:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getCommitted();
            }
        });
        add(new JVMStat("jvm>memory>nonheap>init:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getInit();
            }
        });
        add(new JVMStat("jvm>memory>nonheap>max:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getMax();
            }
        });
        add(new JVMStat("jvm>memory>nonheap>used:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getUsed();
            }
        });
        add(new JVMStat("jvm>gc>collectionTime:millis") {

            @Override
            public long stat() {
                long s = 0;
                for (GarbageCollectorMXBean gc : garbageCollectors) {
                    s += gc.getCollectionTime();
                }
                return s;
            }
        });
        add(new JVMStat("jvm>gc>collectionCount") {

            @Override
            public long stat() {
                long s = 0;
                for (GarbageCollectorMXBean gc : garbageCollectors) {
                    s += gc.getCollectionCount();
                }
                return s;
            }
        });
    }

    public void add(JVMStat jVMStat) {
        stats.add(jVMStat);
    }

    public void logJMVMetrics() {
        for (JVMStat stat : stats) {
            if (stat == null) {
                continue;
            }
            long v = stat.stat();
            logger.set(ValueType.VALUE, stat.key, v);
        }
    }

    public static abstract class JVMStat {

        public String key;

        abstract public long stat();

        public JVMStat(String key) {
            this.key = key;
        }
    }
}
