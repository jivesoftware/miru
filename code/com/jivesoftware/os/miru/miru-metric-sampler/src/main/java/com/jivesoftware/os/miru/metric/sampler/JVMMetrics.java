package com.jivesoftware.os.miru.metric.sampler;

import com.jivesoftware.os.mlogger.core.ValueType;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 *
 * @author jonathan.colt
 */
public class JVMMetrics {

    private final List<GarbageCollectorMXBean> garbageCollectors;
    private final OperatingSystemMXBean osBean;
    private final ThreadMXBean threadBean;
    private final MemoryMXBean memoryBean;
    private final RuntimeMXBean runtimeBean;
    private final List<JVMMetric> metrics;

    public JVMMetrics() {

        garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
        osBean = ManagementFactory.getOperatingSystemMXBean();
        threadBean = ManagementFactory.getThreadMXBean();
        memoryBean = ManagementFactory.getMemoryMXBean();
        runtimeBean = ManagementFactory.getRuntimeMXBean();

        metrics = new LinkedList<>();

        add(new JVMMetric("jvm>numProcs") {

            @Override
            public long stat() {
                return osBean.getAvailableProcessors();
            }
        });

        add(new JVMMetric("jvm>cpuLoad") {

            @Override
            public long stat() {
                try {
                    return (long) getProcessCpuLoad();
                } catch (Exception x) {
                    return -1;
                }
            }
        });
        add(new JVMMetric("jvm>startTime:millis") {

            @Override
            public long stat() {
                return runtimeBean.getStartTime();
            }
        });
        add(new JVMMetric("jvm>upTime:millis") {

            @Override
            public long stat() {
                return runtimeBean.getUptime();
            }
        });
        add(new JVMMetric("jvm>threads>currentThreadCpuTime") {

            @Override
            public long stat() {
                return threadBean.getCurrentThreadCpuTime();
            }
        });
        add(new JVMMetric("jvm>threads>currentThreadUserTime") {

            @Override
            public long stat() {
                return threadBean.getCurrentThreadUserTime();
            }
        });
        add(new JVMMetric("jvm>threads>daemonThreadCount") {

            @Override
            public long stat() {
                return threadBean.getDaemonThreadCount();
            }
        });
        add(new JVMMetric("jvm>threads>peakThreadCount") {

            @Override
            public long stat() {
                return threadBean.getPeakThreadCount();
            }
        });
        add(new JVMMetric("jvm>threads>threadCount") {

            @Override
            public long stat() {
                return threadBean.getThreadCount();
            }
        });
        add(new JVMMetric("jvm>threads>totalStartedThreadCount") {

            @Override
            public long stat() {
                return threadBean.getTotalStartedThreadCount();
            }
        });
        add(new JVMMetric("jvm>memory>heap>commited:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getCommitted();
            }
        });
        add(new JVMMetric("jvm>memory>heap>init:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getInit();
            }
        });
        add(new JVMMetric("jvm>memory>heap>max:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getMax();
            }
        });
        add(new JVMMetric("jvm>memory>heap>used:bytes") {

            @Override
            public long stat() {
                return memoryBean.getHeapMemoryUsage().getUsed();
            }
        });
        add(new JVMMetric("jvm>memory>nonheap>commited:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getCommitted();
            }
        });
        add(new JVMMetric("jvm>memory>nonheap>init:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getInit();
            }
        });
        add(new JVMMetric("jvm>memory>nonheap>max:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getMax();
            }
        });
        add(new JVMMetric("jvm>memory>nonheap>used:bytes") {

            @Override
            public long stat() {
                return memoryBean.getNonHeapMemoryUsage().getUsed();
            }
        });
        add(new JVMMetric("jvm>gc>collectionTime:millis") {

            @Override
            public long stat() {
                long s = 0;
                for (GarbageCollectorMXBean gc : garbageCollectors) {
                    s += gc.getCollectionTime();
                }
                return s;
            }
        });
        add(new JVMMetric("jvm>gc>collectionCount") {

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

    public static double getProcessCpuLoad() throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});

        if (list.isEmpty()) {
            return Double.NaN;
        }

        Attribute att = (Attribute) list.get(0);
        Double value = (Double) att.getValue();

        if (value == -1.0) {
            return Double.NaN;  // usually takes a couple of seconds before we get real values
        }
        return ((int) (value * 1000) / 10.0);        // returns a percentage value with 1 decimal point precision
    }

    public void add(JVMMetric metric) {
        metrics.add(metric);
    }

    public List<AnomalyMetric> sample(String datacenter,
        String cluster,
        String host,
        String service,
        String instance,
        String version) {
        List<AnomalyMetric> anomalyMetric = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        for (JVMMetric m : metrics) {
            if (m == null) {
                continue;
            }
            long v = m.stat();
            anomalyMetric.add(new AnomalyMetric(datacenter,
                cluster,
                host,
                service,
                instance,
                version,
                "null",
                "jvm",
                m.key.split(">"),
                ValueType.VALUE.toString(),
                v,
                String.valueOf(timestamp)));
        }
        return anomalyMetric;
    }

    public static abstract class JVMMetric {

        public String key;

        abstract public long stat();

        public JVMMetric(String key) {
            this.key = key;
        }
    }
}
