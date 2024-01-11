package org.apache.solr.search;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.function.IntPredicate;

public interface JoinIndex {
    default boolean orIntersection(DocIdSetIterator fromDocs, Bits fromLives, Bits toLives, int bufferOffset, FixedBitSet toBuffer) throws IOException {
        if (isEmpty()) {
            return false;
        }
        IntPredicate buff = to -> {
            if (to >= bufferOffset && to < bufferOffset + toBuffer.length()) {
                toBuffer.set(to - bufferOffset);
                return true;
            } else {
                return false;
            }
        };
        return orIntersect(fromDocs, fromLives, toLives, buff);
    }

    boolean orIntersect(DocIdSetIterator fromDocs, Bits fromLives, Bits toLives, IntPredicate buff) throws IOException;

    boolean isEmpty();
}
