package com.jivesoftware.os.miru.plugin.test;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.IndexTx;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MultiIndexTx;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.query.LuceneBackedQueryParser;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 *
 */
public class LuceneBackedQueryParserTest {

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    private final MiruSchema schema = new MiruSchema.Builder("test", 0)
        .setFieldDefinitions(new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "a", MiruFieldDefinition.Type.multiTerm, MiruFieldDefinition.Prefix.WILDCARD),
            new MiruFieldDefinition(1, "b", MiruFieldDefinition.Type.multiTerm, MiruFieldDefinition.Prefix.WILDCARD)
        })
        .build();

    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();
    private final MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
    private final MiruTermComposer termComposer = new MiruTermComposer(StandardCharsets.UTF_8, termInterner);

    private TestFieldIndex fieldIndex;
    private MiruFieldIndexProvider<RoaringBitmap, RoaringBitmap> fieldIndexProvider;
    private TestContext<RoaringBitmap, RoaringBitmap> context;

    @BeforeMethod
    public void setUp() throws Exception {
        fieldIndex = new TestFieldIndex(2, bitmaps);
        @SuppressWarnings("unchecked")
        MiruFieldIndex<RoaringBitmap, RoaringBitmap>[] indexes =
            (MiruFieldIndex<RoaringBitmap, RoaringBitmap>[]) new MiruFieldIndex[MiruFieldType.values().length];
        indexes[0] = fieldIndex;
        fieldIndexProvider = new MiruFieldIndexProvider<>(indexes);
        context = new TestContext<>(schema, termComposer, fieldIndexProvider);
    }

    @Test
    public void testBooleanExpression() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        fieldIndex.put(0, term("red"), RoaringBitmap.bitmapOf(0, 2, 4, 6, 8));
        fieldIndex.put(0, term("green"), RoaringBitmap.bitmapOf(1, 3, 5, 7, 9));
        fieldIndex.put(0, term("blue"), RoaringBitmap.bitmapOf(0, 1, 4, 5, 8, 9));
        fieldIndex.put(0, term("yellow"), RoaringBitmap.bitmapOf(2, 3, 6, 7));

        fieldIndex.put(1, term("yellow"), RoaringBitmap.bitmapOf(0, 2, 4, 6, 8));
        fieldIndex.put(1, term("red"), RoaringBitmap.bitmapOf(1, 3, 5, 7, 9));
        fieldIndex.put(1, term("green"), RoaringBitmap.bitmapOf(0, 1, 4, 5, 8, 9));
        fieldIndex.put(1, term("blue"), RoaringBitmap.bitmapOf(2, 3, 6, 7));

        LuceneBackedQueryParser parser = new LuceneBackedQueryParser("a");

        MiruFilter filter = parser.parse("en", false, "(red AND b:blue) OR (b:yellow NOT yellow)");
        // ((0, 2, 4, 6, 8) AND (2, 3, 6, 7)) OR ((0, 2, 4, 6, 8) NOT (2, 3, 6, 7))
        // (2, 6) OR (0, 4, 8)
        // (0, 2, 4, 6, 8)
        MiruSolutionLog solutionLog = new MiruSolutionLog(MiruSolutionLogLevel.NONE);
        RoaringBitmap storage = aggregateUtil.filter("test", bitmaps, context, filter, solutionLog, null, 9, -1, -1, stackBuffer);
        Assert.assertEquals(storage.getCardinality(), 5);
        assertTrue(storage.contains(0));
        assertTrue(storage.contains(2));
        assertTrue(storage.contains(4));
        assertTrue(storage.contains(6));
        assertTrue(storage.contains(8));
    }

    @Test
    public void testWildcardExpression() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        fieldIndex.put(0, term("red"), RoaringBitmap.bitmapOf(0, 2, 4, 6, 8));
        fieldIndex.put(0, term("green"), RoaringBitmap.bitmapOf(1, 3, 5, 7, 9));
        fieldIndex.put(0, term("blue"), RoaringBitmap.bitmapOf(0, 1, 4, 5, 8, 9));
        fieldIndex.put(0, term("yellow"), RoaringBitmap.bitmapOf(2, 3, 6, 7));

        fieldIndex.put(1, term("yellow"), RoaringBitmap.bitmapOf(0, 2, 4, 6, 8));
        fieldIndex.put(1, term("red"), RoaringBitmap.bitmapOf(1, 3, 5, 7, 9));
        fieldIndex.put(1, term("green"), RoaringBitmap.bitmapOf(0, 1, 4, 5, 8, 9));
        fieldIndex.put(1, term("blue"), RoaringBitmap.bitmapOf(2, 3, 6, 7));

        LuceneBackedQueryParser parser = new LuceneBackedQueryParser("a");

        MiruFilter filter = parser.parse("en", false, "(re* AND b:bl*) OR (b:ye* NOT ye*)");
        // ((0, 2, 4, 6, 8) AND (2, 3, 6, 7)) OR ((0, 2, 4, 6, 8) NOT (2, 3, 6, 7))
        // (2, 6) OR (0, 4, 8)
        // (0, 2, 4, 6, 8)
        MiruSolutionLog solutionLog = new MiruSolutionLog(MiruSolutionLogLevel.NONE);
        RoaringBitmap storage = aggregateUtil.filter("test", bitmaps, context, filter, solutionLog, null, 9, -1, -1, stackBuffer);
        Assert.assertEquals(storage.getCardinality(), 5);
        assertTrue(storage.contains(0));
        assertTrue(storage.contains(2));
        assertTrue(storage.contains(4));
        assertTrue(storage.contains(6));
        assertTrue(storage.contains(8));
    }

    private MiruTermId term(String term) {
        return new MiruTermId(term.getBytes(StandardCharsets.UTF_8));
    }

    private static class TestFieldIndex implements MiruFieldIndex<RoaringBitmap, RoaringBitmap> {

        private final NavigableMap<MiruTermId, RoaringBitmap>[] indexes;
        private final MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps;

        public TestFieldIndex(int numFields, MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps) {
            indexes = new NavigableMap[numFields];
            this.bitmaps = bitmaps;
            for (int i = 0; i < numFields; i++) {
                Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
                indexes[i] = new ConcurrentSkipListMap<>((o1, o2) -> {
                    return comparator.compare(o1.getBytes(), o2.getBytes());
                });
            }
        }

        public void put(int fieldId, MiruTermId termId, RoaringBitmap bitmap) {
            indexes[fieldId].put(termId, bitmap);
        }

        @Override
        public MiruInvertedIndex<RoaringBitmap, RoaringBitmap> get(String name, int fieldId, MiruTermId termId) throws Exception {
            return new TestInvertedIndex(fieldId, termId, bitmaps);
        }

        @Override
        public void streamTermIdsForField(String name,
            int fieldId,
            List<KeyRange> ranges,
            TermIdStream termIdStream,
            StackBuffer stackBuffer) throws Exception {
            for (KeyRange range : ranges) {
                MiruTermId fromKey = new MiruTermId(range.getStartInclusiveKey());
                MiruTermId toKey = new MiruTermId(range.getStopExclusiveKey());
                for (MiruTermId termId : indexes[fieldId].subMap(fromKey, toKey).keySet()) {
                    if (!termIdStream.stream(termId)) {
                        break;
                    }
                }
            }
        }

        @Override
        public MiruInvertedIndex<RoaringBitmap, RoaringBitmap> getOrCreateInvertedIndex(String name, int fieldId, MiruTermId term) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public void multiGet(String name,
            int fieldId,
            MiruTermId[] termIds,
            BitmapAndLastId<RoaringBitmap>[] results,
            StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public void multiGetLastIds(String name, int fieldId, MiruTermId[] termIds, int[] lastIds, StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public void multiTxIndex(String name,
            int fieldId,
            MiruTermId[] termIds,
            int considerIfLastIdGreaterThanN,
            StackBuffer stackBuffer,
            MultiIndexTx<RoaringBitmap> indexTx) throws Exception {
            NavigableMap<MiruTermId, RoaringBitmap> terms = indexes[fieldId];
            for (int i = 0; i < termIds.length; i++) {
                MiruTermId termId = termIds[i];
                if (termId != null) {
                    RoaringBitmap bitmap = terms.get(termId);
                    if (bitmap != null) {
                        IntIterator reverse = bitmap.getReverseIntIterator();
                        int lastId = reverse.hasNext() ? reverse.next() : -1;
                        indexTx.tx(i, lastId, bitmap, null, -1, stackBuffer);
                    }
                }
            }
        }

        @Override
        public void set(MiruFieldDefinition fieldDefinition, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public void setIfEmpty(MiruFieldDefinition fieldDefinition, MiruTermId termId, int id, long count, StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public void remove(MiruFieldDefinition fieldDefinition, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public long getCardinality(MiruFieldDefinition fieldDefinition, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public long[] getCardinalities(MiruFieldDefinition fieldDefinition, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        @Override
        public long getGlobalCardinality(MiruFieldDefinition fieldDefinition, MiruTermId termId, StackBuffer stackBuffer) throws Exception {
            throw new UnsupportedOperationException("Nope");
        }

        private class TestInvertedIndex implements MiruInvertedIndex<RoaringBitmap, RoaringBitmap> {

            private final int fieldId;
            private final MiruTermId termId;
            private final MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps;

            public TestInvertedIndex(int fieldId, MiruTermId termId, MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps) {
                this.fieldId = fieldId;
                this.termId = termId;
                this.bitmaps = bitmaps;
            }

            @Override
            public void getIndex(BitmapAndLastId<RoaringBitmap> container, StackBuffer stackBuffer) throws Exception {
                RoaringBitmap bitmap = indexes[fieldId].get(termId);
                container.set(bitmap, bitmaps.lastSetBit(bitmap));
            }

            @Override
            public void remove(StackBuffer stackBuffer, int... ids) throws Exception {
            }

            @Override
            public void set(StackBuffer stackBuffer, int... ids) throws Exception {
            }

            @Override
            public boolean setIfEmpty(StackBuffer stackBuffer, int id) throws Exception {
                return false;
            }

            @Override
            public int lastId(StackBuffer stackBuffer) throws Exception {
                return 0;
            }

            @Override
            public void andNotToSourceSize(List<RoaringBitmap> masks, StackBuffer stackBuffer) throws Exception {
            }

            @Override
            public void orToSourceSize(RoaringBitmap mask, StackBuffer stackBuffer) throws Exception {
            }

            @Override
            public void andNot(RoaringBitmap mask, StackBuffer stackBuffer) throws Exception {
            }

            @Override
            public void or(RoaringBitmap mask, StackBuffer stackBuffer) throws Exception {
            }

            @Override
            public <R> R txIndex(IndexTx<R, RoaringBitmap> tx, StackBuffer stackBuffer) throws Exception {
                return tx.tx(indexes[fieldId].get(termId), null, -1, null);
            }
        }
    }

}
