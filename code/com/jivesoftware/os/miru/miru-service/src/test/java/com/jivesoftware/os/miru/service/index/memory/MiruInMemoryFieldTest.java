package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.jive.utils.map.store.VariableKeySizeBytesObjectMapStore;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class MiruInMemoryFieldTest {

    @Test(enabled = false)
    public void testMapComparisons() throws Exception {
        final boolean doConcurrentMap = true;
        final boolean doMapStore = true;
        final long sleepOnCompletion = 0;

        final int numIterations = 10;
        final int numTerms = 1_000_000;
        MiruFieldIndexKey[] fieldIndexKeys = new MiruFieldIndexKey[numTerms];
        for (int i = 0; i < fieldIndexKeys.length; i++) {
            fieldIndexKeys[i] = new MiruFieldIndexKey(i, 0);
        }

        for (int i = 0; i < numIterations; i++) {
            System.out.println("---------------------- " + i + " ----------------------");

            if (doConcurrentMap) {
                // concurrent map setup
                ConcurrentMap<MiruTermId, MiruFieldIndexKey> concurrentMap = new ConcurrentHashMap<>(
                    10, 0.5f, 64);

                // concurrent map insert
                long start = System.currentTimeMillis();
                for (int termId = 0; termId < numTerms; termId++) {
                    MiruTermId key = new MiruTermId(bytesForTerm(termId));
                    concurrentMap.put(key, fieldIndexKeys[termId]);
                }
                System.out.println("ConcurrentHashMap: Inserted " + concurrentMap.size() + " in " + (System.currentTimeMillis() - start) + "ms");

                // concurrent map retrieve
                start = System.currentTimeMillis();
                for (int termId = 0; termId < numTerms; termId++) {
                    MiruTermId key = new MiruTermId(bytesForTerm(termId));
                    MiruFieldIndexKey retrieved = concurrentMap.get(key);
                    assertTrue(retrieved.getId() == fieldIndexKeys[termId].getId(), "Failed at " + termId);
                }
                if (i == numIterations - 1 && sleepOnCompletion > 0) {
                    Thread.sleep(sleepOnCompletion);
                }
                System.out.println("ConcurrentHashMap: Retrieved " + concurrentMap.size() + " in " + (System.currentTimeMillis() - start) + "ms");
            }

            if (doMapStore) {
                // bytebuffer mapstore setup
                VariableKeySizeBytesObjectMapStore<MiruTermId, MiruFieldIndexKey> mapStore =
                    new VariableKeySizeBytesObjectMapStore<MiruTermId, MiruFieldIndexKey>(new int[] { 2, 4, 8, 16 }, 10, null) {

                        @Override
                        protected int keyLength(MiruTermId key) {
                            return key.getBytes().length;
                        }

                        @Override
                        public byte[] keyBytes(MiruTermId key) {
                            return key.getBytes();
                        }

                        @Override
                        public MiruTermId bytesKey(byte[] bytes, int offset) {
                            return new MiruTermId(bytes);
                        }
                    };

                // bytebuffer mapstore insert
                long start = System.currentTimeMillis();
                for (int termId = 0; termId < numTerms; termId++) {
                    MiruTermId key = new MiruTermId(bytesForTerm(termId));
                    mapStore.add(key, fieldIndexKeys[termId]);
                }
                System.out.println("VariableKeySizeBytesObjectMapStore: Inserted " + mapStore.estimatedMaxNumberOfKeys() + " in " +
                    (System.currentTimeMillis() - start) + "ms");

                // bytebuffer mapstore retrieve
                start = System.currentTimeMillis();
                for (int termId = 0; termId < numTerms; termId++) {
                    MiruTermId key = new MiruTermId(bytesForTerm(termId));
                    MiruFieldIndexKey retrieved = mapStore.get(key);
                    assertTrue(retrieved.getId() == fieldIndexKeys[termId].getId(), "Failed at " + termId);
                }
                if (i == numIterations - 1 && sleepOnCompletion > 0) {
                    Thread.sleep(sleepOnCompletion);
                }
                System.out.println("VariableKeySizeBytesObjectMapStore: Retrieved " + mapStore.estimatedMaxNumberOfKeys() + " in " +
                    (System.currentTimeMillis() - start) + "ms");
            }
        }
    }

    private byte[] bytesForTerm(int termId) {
        return String.valueOf(termId).getBytes();
    }
}