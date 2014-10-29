package com.jivesoftware.os.miru.service.proof;

import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/** @author jonathan */
public class MiruStreamsPOC {

    public static void main(String[] args) {
        Random rand = new Random(123_345);

        MiruStreamsPOC streamsPOC = new MiruStreamsPOC();

        int numDocs = 1_000_000;
        int[] numTermsPerField = new int[] { 1_000, 1_000, 1_000, 10_000, 100_000, 1_000_000 };

        for (int docId = 0; docId < numDocs; docId++) {

            int id = docId;

            for (int f = 0; f < 4; f++) {
                int field = rand.nextInt(numTermsPerField.length);
                int term = rand.nextInt(numTermsPerField[field]);
                streamsPOC.add(id, field, new MiruTermId[] { new MiruTermId(FilerIO.intBytes(term)) });
            }
            if (docId % 1_000 == 0) {
                System.out.println("indexed " + docId);
            }
        }

        //        for(Long key:streamsPOC.index.keySet()) {
        //            System.out.println("key:"+key+" count:"+streamsPOC.index.get(key).cardinality());
        //        }
        for (int i = 0; i < 1; i++) {
            long time = System.currentTimeMillis();
            List<FieldQuery> fieldQueries = new ArrayList<>();
            int numberOfFollowed = 10_000;
            for (int f = 0; f < numberOfFollowed; f++) {
                int field = rand.nextInt(numTermsPerField.length);
                int term = rand.nextInt(numTermsPerField[field]);
                fieldQueries.add(new FieldQuery(field, new MiruTermId[] { new MiruTermId(FilerIO.intBytes(term)) }));
            }
            EWAHCompressedBitmap result = streamsPOC.query(fieldQueries, 1_000);
            long latency = (System.currentTimeMillis() - time);
            for (int r : result.toArray()) {
                System.out.println("r:" + r);
            }
            System.out.println("Count:" + result.cardinality() + " latency:" + latency + " postingListCount:" + streamsPOC.index.size());
        }
    }

    private final Map<FieldAndTermId, EWAHCompressedBitmap> index = new ConcurrentHashMap<>();

    public void add(int docId, int fieldId, MiruTermId[] termIds) {
        for (MiruTermId termId : termIds) {
            FieldAndTermId key = new FieldAndTermId(fieldId, termId);
            getOrAllocate(key).set(docId);
        }
    }

    EWAHCompressedBitmap getOrAllocate(FieldAndTermId key) {
        EWAHCompressedBitmap got = index.get(key);
        if (got == null) {
            got = new EWAHCompressedBitmap();
            index.put(key, got);
        }
        return got;
    }

    EWAHCompressedBitmap query(List<FieldQuery> fieldQuerys, int resultCount) {
        List<EWAHCompressedBitmap> bitmaps = new ArrayList<>();
        for (FieldQuery fieldQuery : fieldQuerys) {
            for (MiruTermId termId : fieldQuery.termIds) {
                FieldAndTermId key = new FieldAndTermId(fieldQuery.fieldId, termId);
                EWAHCompressedBitmap got = index.get(key);
                if (got != null) {
                    bitmaps.add(got);
                }
            }
        }
        System.out.println("Query fieldQuerys:" + fieldQuerys.size() + " lookingIn:" + bitmaps.size());
        EWAHCompressedBitmap answer = new EWAHCompressedBitmap();
        if (resultCount < 1) {
            FastAggregation.bufferedorWithContainer(answer, 1_024, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
        } else {
            BitmapCollector collector = new BitmapCollector(answer, resultCount);
            try {
                FastAggregation.bufferedorWithContainer(collector, 1_024, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
            } catch (StopCollecting sc) {
            }
        }
        return answer;

    }

    static class StopCollecting extends RuntimeException { // SUCKS to use exceptions for flow control but not much else we can do :(

    }

    static class BitmapCollector implements BitmapStorage {
        private final BitmapStorage delegateStoreage;
        private final int maxHitCount;
        private int oneBitsHits;

        BitmapCollector(BitmapStorage delegateStoreage, int maxHitCount) {
            this.delegateStoreage = delegateStoreage;
            this.maxHitCount = maxHitCount;
        }

        @Override
        public void addWord(final long newdata) {
            this.oneBitsHits += Long.bitCount(newdata);
            delegateStoreage.addWord(newdata);
            if (oneBitsHits > maxHitCount) {
                throw new StopCollecting();
            }
        }

        @Override
        public void addStreamOfLiteralWords(long[] data, int start, int number) {
            for (int i = start; i < start + number; i++) {
                addWord(data[i]);
            }
            delegateStoreage.addStreamOfLiteralWords(data, start, number);
        }

        @Override
        public void addStreamOfEmptyWords(boolean v, long number) {
            if (v) {
                this.oneBitsHits += number * EWAHCompressedBitmap.WORD_IN_BITS;
                if (oneBitsHits > maxHitCount) {
                    throw new StopCollecting();
                }
            }
            delegateStoreage.addStreamOfEmptyWords(v, number);
        }

        @Override
        public void addStreamOfNegatedLiteralWords(long[] data, int start, int number) {
            for (int i = start; i < start + number; i++) {
                addWord(~data[i]);
            }
            delegateStoreage.addStreamOfNegatedLiteralWords(data, start, number);
        }

        public int getCount() {
            return oneBitsHits;
        }

        @Override
        public void setSizeInBitsWithinLastWord(int bits) {
            delegateStoreage.setSizeInBitsWithinLastWord(bits);
        }

        @Override
        public void clear() {
            oneBitsHits = 0;
            delegateStoreage.clear();
        }
    }

    static class FieldQuery {

        int fieldId;
        MiruTermId[] termIds;

        FieldQuery(int fieldId, MiruTermId[] termIds) {
            this.fieldId = fieldId;
            this.termIds = termIds;
        }

    }

    private static class FieldAndTermId {

        public final int fieldId;
        public final MiruTermId termID;

        private FieldAndTermId(int fieldId, MiruTermId termID) {
            this.fieldId = fieldId;
            this.termID = termID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FieldAndTermId that = (FieldAndTermId) o;

            if (fieldId != that.fieldId) {
                return false;
            }
            if (termID != null ? !termID.equals(that.termID) : that.termID != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = fieldId;
            result = 31 * result + (termID != null ? termID.hashCode() : 0);
            return result;
        }
    }

}
