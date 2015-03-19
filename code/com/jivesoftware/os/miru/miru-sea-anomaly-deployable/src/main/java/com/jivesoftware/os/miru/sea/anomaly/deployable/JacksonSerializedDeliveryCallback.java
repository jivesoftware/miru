package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.DeliveryCallback;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
abstract class JacksonSerializedDeliveryCallback<T> implements DeliveryCallback {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    final int maxDrainSize;
    final ObjectMapper objectMapper;
    final Class<T> serializationClass;
    final boolean continueOnSerializationFailure;
    final Comparator<T> comparator;

    protected JacksonSerializedDeliveryCallback(int maxDrainSize,
        ObjectMapper objectMapper,
        Class<T> serializationClass,
        boolean continueOnSerializationFailure,
        Comparator<T> comparator) {
        this.maxDrainSize = maxDrainSize;
        this.objectMapper = objectMapper;
        this.serializationClass = serializationClass;
        this.continueOnSerializationFailure = continueOnSerializationFailure;
        this.comparator = comparator;
    }

    @Override
    public boolean deliver(Iterable<byte[]> bytes) {
        List<T> serialized = new ArrayList<>(maxDrainSize);
        for (byte[] serialBytes : bytes) {
            try {
                serialized.add(objectMapper.readValue(serialBytes, serializationClass));
            } catch (IOException x) {
                LOG.error("Encountered the following while deserializing.", x);
                if (!continueOnSerializationFailure) {
                    return false;
                }
            }
            if (serialized.size() == maxDrainSize) {
                if (comparator != null) {
                    // sort for better time ordering
                    Collections.sort(serialized, comparator);
                }
                deliverSerialized(serialized);
                serialized.clear();
            }
        }
        if (!serialized.isEmpty()) {
            deliverSerialized(serialized);
        }
        return true;
    }

    abstract void deliverSerialized(List<T> serialized);

}

