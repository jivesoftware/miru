package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;

/**
 *
 */
public class MiruFilterUtils {

    public MiruTermId makeComposite(MiruTermId fieldValue, String separator, String fieldName) {
        return new MiruTermId(Bytes.concat(fieldValue.getBytes(), separator.getBytes(), fieldName.getBytes()));
    }

    public <BM> void stream(MiruBitmaps<BM> bitmaps,
            MiruTenantId tenantId,
            MiruQueryStream stream,
            BM answer,
            Optional<BM> counter,
            MiruField<BM> pivotField,
            String streamField,
            CallbackStream<TermCount> terms)
            throws Exception {

        final AtomicLong bytesTraversed = new AtomicLong();
        bytesTraversed.addAndGet(bitmaps.sizeInBytes(answer));
        CardinalityAndLastSetBit answerCollector = null;
        ReusableBuffers<BM> reusable = new ReusableBuffers<>(bitmaps, 2);
        int fieldId = stream.schema.getFieldId(streamField);
        long beforeCount = counter.isPresent() ? bitmaps.cardinality(counter.get()) : bitmaps.cardinality(answer);
        while (true) {
            int lastSetBit = answerCollector == null ? bitmaps.lastSetBit(answer) : answerCollector.lastSetBit;
            if (lastSetBit < 0) {
                break;
            }

            MiruTermId[] fieldValues = stream.activityIndex.get(tenantId, lastSetBit, fieldId);
            if (fieldValues == null || fieldValues.length == 0) {
                // could make this a reusable buffer, but this is effectively an error case and would require 3 buffers
                BM removeUnknownField = bitmaps.create();
                bitmaps.set(removeUnknownField, lastSetBit);
                bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(removeUnknownField)));

                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, removeUnknownField);
                answer = revisedAnswer;

            } else {
                MiruTermId pivotTerm = fieldValues[0]; // Kinda lame but for now we don't see a need for multi field aggregation.

                Optional<MiruInvertedIndex<BM>> invertedIndex = pivotField.getInvertedIndex(pivotTerm);
                checkState(invertedIndex.isPresent(), "Unable to load inverted index for aggregateTermId: " + pivotTerm);

                BM termIndex = invertedIndex.get().getIndex();
                bytesTraversed.addAndGet(Math.max(bitmaps.sizeInBytes(answer), bitmaps.sizeInBytes(termIndex)));

                BM revisedAnswer = reusable.next();
                answerCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedAnswer, answer, termIndex);
                answer = revisedAnswer;

                long afterCount;
                if (counter.isPresent()) {
                    BM revisedCounter = reusable.next();
                    CardinalityAndLastSetBit counterCollector = bitmaps.andNotWithCardinalityAndLastSetBit(revisedCounter, counter.get(), termIndex);
                    counter = Optional.of(revisedCounter);
                    afterCount = counterCollector.cardinality;
                } else {
                    afterCount = answerCollector.cardinality;
                }

                TermCount termCount = new TermCount(pivotTerm, fieldValues, beforeCount - afterCount);
                if (termCount != terms.callback(termCount)) { // Stop stream
                    return;
                }
                beforeCount = afterCount;
            }

        }
        terms.callback(null); // EOS
        System.out.println("Bytes Traversed=" + bytesTraversed.longValue());

    }

}
