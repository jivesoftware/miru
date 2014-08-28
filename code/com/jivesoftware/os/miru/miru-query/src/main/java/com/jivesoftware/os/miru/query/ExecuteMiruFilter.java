package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.Charsets;

/**
 * @author jonathan
 */
public class ExecuteMiruFilter<BM> implements Callable<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final MiruSchema schema;
    private final MiruFields<BM> fieldIndex;
    private final ExecutorService executorService;
    private final MiruFilter filter;
    private final Optional<BM> bitmapStorage;
    private final int considerIfIndexIdGreaterThanN;

    public ExecuteMiruFilter(MiruBitmaps<BM> bitmaps,
            MiruSchema schema,
            MiruFields<BM> fieldIndex,
            ExecutorService executorService,
            MiruFilter filter,
            Optional<BM> bitmapStorage,
            int considerIfIndexIdGreaterThanN) {

        this.bitmaps = bitmaps;
        this.schema = schema;
        this.fieldIndex = fieldIndex;
        this.executorService = executorService;
        this.filter = filter;
        this.bitmapStorage = bitmapStorage;
        this.considerIfIndexIdGreaterThanN = considerIfIndexIdGreaterThanN;
    }

    @Override
    public BM call() throws Exception {
        List<BM> filterBitmaps = new ArrayList<>();
        if (filter.fieldFilters.isPresent()) {
            for (MiruFieldFilter fieldFilter : filter.fieldFilters.get()) {
                int fieldId = schema.getFieldId(fieldFilter.fieldName);
                if (fieldId >= 0) {
                    MiruField<BM> field = fieldIndex.getField(fieldId);
                    List<BM> fieldBitmaps = new ArrayList<>();
                    for (String term : fieldFilter.values) {
                        Optional<MiruInvertedIndex<BM>> got = field.getInvertedIndex(
                                new MiruTermId(term.getBytes(Charsets.UTF_8)),
                                considerIfIndexIdGreaterThanN);
                        if (got.isPresent()) {
                            fieldBitmaps.add(got.get().getIndex());
                        }
                    }
                    if (fieldBitmaps.isEmpty() && filter.operation == MiruFilterOperation.and) {
                        // implicitly empty results, "and" operation would also be empty
                        return bitmaps.create();
                    } else if (!fieldBitmaps.isEmpty()) {
                        BM r = bitmaps.create();
                        bitmaps.or(r, fieldBitmaps);
                        filterBitmaps.add(r);
                    }
                }
            }
        }
        if (filter.subFilter.isPresent()) {
            //TODO rethink this, submitting to executor deadlocks if all threads are waiting on a subfilter to execute, prevents use of fixed-size thread pool
            //List<Future<BM>> futures = new ArrayList<>();
            for (MiruFilter subFilter : filter.subFilter.get()) {
                // could use hit collector?
                ExecuteMiruFilter<BM> executeFilter = new ExecuteMiruFilter<>(bitmaps, schema, fieldIndex, executorService, subFilter, Optional.<BM>absent(),
                        considerIfIndexIdGreaterThanN);
                filterBitmaps.add(executeFilter.call());
                //futures.add(executorService.submit(executeFilter));
            }
            /*
            for (Future<BM> future : futures) {
                bitmaps.add(future.get());
            }
            */
        }
        return execute(filterBitmaps);
    }

    private BM execute(List<BM> filterBitmaps) {
        if (bitmapStorage.isPresent()) {
            if (filterBitmaps.isEmpty()) {
                throw new StopCollecting(); // TODO kill this exception
            }
            try {
                if (filter.operation == MiruFilterOperation.and) {
                    bitmaps.and(bitmapStorage.get(), filterBitmaps);
                } else if (filter.operation == MiruFilterOperation.or) {
                    bitmaps.or(bitmapStorage.get(), filterBitmaps);
                } else if (filter.operation == MiruFilterOperation.pButNotQ) {
                    bitmaps.andNot(bitmapStorage.get(), filterBitmaps.get(0), filterBitmaps.subList(1, filterBitmaps.size()));
                } else {
                    throw new UnsupportedOperationException(filter.operation + " isn't currently supported.");
                }
            } catch (StopCollecting sc) {
            }
            return null;
        } else {
            if (filterBitmaps.isEmpty()) {
                return bitmaps.create();
            }
            if (filter.operation == MiruFilterOperation.and) {
                BM r = bitmaps.create();
                bitmaps.and(r, filterBitmaps);
                return r;
            } else if (filter.operation == MiruFilterOperation.or) {
                BM r = bitmaps.create();
                bitmaps.or(r, filterBitmaps);
                return r;
            } else if (filter.operation == MiruFilterOperation.pButNotQ) {
                BM r = bitmaps.create();
                bitmaps.andNot(r, filterBitmaps.get(0), filterBitmaps.subList(1, filterBitmaps.size()));
                return r;
            } else {
                throw new UnsupportedOperationException(filter.operation + " isn't currently supported.");
            }
        }
    }

}
