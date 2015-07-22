package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Maps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.io.FileUtils;

/**
 *
 */
public class BerkeleyKeyedIndexStore implements KeyedIndexStore {

    private final File envPath;
    private final Environment environment;

    private final Map<String, BerkeleyKeyedIndex> keyedIndexes = Maps.newConcurrentMap();

    public BerkeleyKeyedIndexStore(File[] indexStoreDirs, int directoryOffset, String name, EnvironmentConfig environmentConfig) {
        this.envPath = new File(indexStoreDirs[directoryOffset % indexStoreDirs.length], name);
        this.envPath.mkdirs();
        this.environment = new Environment(envPath, environmentConfig);
        System.out.println("Opened berkeley index store in " + envPath);
    }

    @Override
    public void close() {
        for (BerkeleyKeyedIndex keyedIndex : keyedIndexes.values()) {
            keyedIndex.close();
        }
        environment.close();
    }

    @Override
    public void delete() throws IOException {
        FileUtils.deleteDirectory(envPath);
    }

    @Override
    public void flush(boolean fsync) {
        environment.flushLog(fsync);
    }

    @Override
    public KeyedIndex open(String indexName) {
        return keyedIndexes.computeIfAbsent(indexName, key -> {
            DatabaseConfig dbConfig = new DatabaseConfig().setAllowCreate(true);
            return new BerkeleyKeyedIndex(environment.openDatabase(null, indexName, dbConfig));
        });
    }

    @Override
    public void copyTo(KeyedIndexStore indexStore) {
        for (String indexName : environment.getDatabaseNames()) {
            KeyedIndex from = open(indexName);
            KeyedIndex to = indexStore.open(indexName);
            from.copyTo(to);
        }
    }
}
