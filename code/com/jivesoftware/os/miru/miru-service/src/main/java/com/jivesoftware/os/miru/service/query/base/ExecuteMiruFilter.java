package com.jivesoftware.os.miru.service.query.base;

import com.google.common.base.Optional;
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruFields;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.partition.MiruBitsAggregation;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/** @author jonathan */
public class ExecuteMiruFilter implements Callable<EWAHCompressedBitmap> {

    private final MiruSchema schema;
    private final MiruFields fieldIndex;
    private final ExecutorService executorService;
    private final MiruFilter filter;
    private final Optional<BitmapStorage> bitmapStorage;
    private final int considerIfIndexIdGreaterThanN;
    private final int bitsetBufferSize;

    public ExecuteMiruFilter(MiruSchema schema,
        MiruFields fieldIndex,
        ExecutorService executorService,
        MiruFilter filter,
        Optional<BitmapStorage> bitmapStorage,
        int considerIfIndexIdGreaterThanN,
        int bitsetBufferSize) {
        this.schema = schema;
        this.fieldIndex = fieldIndex;
        this.executorService = executorService;
        this.filter = filter;
        this.bitmapStorage = bitmapStorage;
        this.considerIfIndexIdGreaterThanN = considerIfIndexIdGreaterThanN;
        this.bitsetBufferSize = bitsetBufferSize;
    }

    @Override
    public EWAHCompressedBitmap call() throws Exception {
        List<EWAHCompressedBitmap> bitmaps = new ArrayList<>();
        if (filter.fieldFilters.isPresent()) {
            for (MiruFieldFilter fieldFilter : filter.fieldFilters.get()) {
                int fieldId = schema.getFieldId(fieldFilter.fieldName);
                if (fieldId >= 0) {
                    MiruField field = fieldIndex.getField(fieldId);
                    List<EWAHCompressedBitmap> fieldBitmaps = new ArrayList<>();
                    for (MiruTermId term : fieldFilter.values) {
                        Optional<MiruInvertedIndex> got = field.getInvertedIndex(term, considerIfIndexIdGreaterThanN);
                        if (got.isPresent()) {
                            fieldBitmaps.add(got.get().getIndex());
                        }
                    }
                    if (fieldBitmaps.isEmpty() && filter.operation == MiruFilterOperation.and) {
                        // implicitly empty results, "and" operation would also be empty
                        return new EWAHCompressedBitmap();
                    } else if (!fieldBitmaps.isEmpty()) {
                        bitmaps.add(FastAggregation.bufferedor(bitsetBufferSize, fieldBitmaps.toArray(new EWAHCompressedBitmap[fieldBitmaps.size()])));
                    }
                }
            }
        }
        if (filter.subFilter.isPresent()) {
            //TODO rethink this, submitting to executor deadlocks if all threads are waiting on a subfilter to execute, prevents use of fixed-size thread pool
            //List<Future<EWAHCompressedBitmap>> futures = new ArrayList<>();
            for (MiruFilter subFilter : filter.subFilter.get()) {
                // could use hit collector?
                ExecuteMiruFilter executeFilter = new ExecuteMiruFilter(schema, fieldIndex, executorService, subFilter, Optional.<BitmapStorage>absent(),
                    considerIfIndexIdGreaterThanN, bitsetBufferSize);
                bitmaps.add(executeFilter.call());
                //futures.add(executorService.submit(executeFilter));
            }
            /*
            for (Future<EWAHCompressedBitmap> future : futures) {
                bitmaps.add(future.get());
            }
            */
        }
        return execute(bitmaps);
    }

    private EWAHCompressedBitmap execute(List<EWAHCompressedBitmap> bitmaps) {
        if (bitmapStorage.isPresent()) {
            if (bitmaps.isEmpty()) {
                throw new StopCollecting(); // TODO kill this exception
            }
            try {
                if (filter.operation == MiruFilterOperation.and) {
                    FastAggregation.bufferedandWithContainer(bitmapStorage.get(), bitsetBufferSize, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
                } else if (filter.operation == MiruFilterOperation.or) {
                    FastAggregation.bufferedorWithContainer(bitmapStorage.get(), bitsetBufferSize, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
                } else if (filter.operation == MiruFilterOperation.pButNotQ) {
                    MiruBitsAggregation.pButNotQ(bitmapStorage.get(), bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
                } else {
                    throw new UnsupportedOperationException(filter.operation + " isn't currently supported.");
                }
            } catch (StopCollecting sc) {
            }
            return null;
        } else {
            if (bitmaps.isEmpty()) {
                return new EWAHCompressedBitmap();
            }
            if (filter.operation == MiruFilterOperation.and) {
                return FastAggregation.bufferedand(bitsetBufferSize, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
            } else if (filter.operation == MiruFilterOperation.or) {
                return FastAggregation.bufferedor(bitsetBufferSize, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
            } else if (filter.operation == MiruFilterOperation.pButNotQ) {
                EWAHCompressedBitmap answer = new EWAHCompressedBitmap();
                MiruBitsAggregation.pButNotQ(answer, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
                return answer;
            } else {
                throw new UnsupportedOperationException(filter.operation + " isn't currently supported.");
            }
        }
    }

}
