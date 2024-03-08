package org.apache.solr.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class ParArrJoinIndex implements JoinIndex{
    private final int[] fromDocs;
    private final int[] toDocs;
    private final int tuples;

    @SuppressWarnings("unchecked")
    public ParArrJoinIndex(String fromField, LeafReaderContext fromCtx, String toField, LeafReaderContext toCtx) throws IOException {
        // scratch might cary filled cells as well
        List<Integer>[] scratch = new ArrayJoinIndex.Intersector(fromField, fromCtx, toField, toCtx).intersect();
        if (scratch!=null) {
            int[] inOrder = new int[scratch.length];
            int[] outOfOrder = new int[scratch.length];
            int outpos=0;
            for (int i = 0; i < scratch.length; i++) {
                if (scratch[i]!=null) {
                    for (int toDocNum:scratch[i]) { // notnull
                        if (outpos>=inOrder.length) {
                            inOrder = ArrayUtil.grow(inOrder);
                            outOfOrder = ArrayUtil.grow(outOfOrder, inOrder.length);
                        }
                        inOrder[outpos]=i;
                        outOfOrder[outpos]=toDocNum;
                        outpos++;
                    }
                }
            }
            this.fromDocs = inOrder;
            this.toDocs = outOfOrder;
            this.tuples = outpos;
        } else {
            this.fromDocs = null;
            this.toDocs = null;
            this.tuples = -1;
        }
    }

    @Override
    public boolean orIntersect(DocIdSetIterator fromDocs, Bits fromLives, Bits toLives, IntPredicate buff) throws IOException {
        boolean hit = false;
        if (this.tuples<=0){
            return false;
        }
        int fromFromPos=0;
        for (int from=this.fromDocs[0]; (from = fromDocs.advance(from)) != NO_MORE_DOCS; ) {
            if (fromLives != null) {
                if (!fromLives.get(from)) {
                    from++;
                    continue;
                }
            }
            int pos = Arrays.binarySearch(this.fromDocs, fromFromPos, this.tuples, from);
            if (pos>=0) { //found.
                // rewind equal
                while(pos>=1 && this.fromDocs[pos-1]==from){
                    pos--;
                } 
                for (;pos<this.tuples && this.fromDocs[pos]==from;pos++) {
                    // TODO hell, we need to drag from scorer and push score into as well.
                    int toDocNum = this.toDocs[pos];
                    if (toLives != null) {
                        if (!toLives.get(toDocNum)) {
                            continue;
                        }
                    }
                    hit |= buff.test(toDocNum);
                }//over
            } else {
                // not found advance
                // (-(insertion point) - 1).
                pos = -pos-1;
            }
            if (pos>=this.tuples){
                return hit;
            }
            from = this.fromDocs[pos];
            fromFromPos=pos;
        }
        return hit;
    }

    @Override
    public boolean isEmpty() {
        return fromDocs==null || toDocs==null || this.tuples<=0;
    }

    @Override
    public JoinIndex reverse() {
        return new JoinIndex() {
            @Override
            public boolean orIntersect(DocIdSetIterator fromDocs, Bits fromLives, Bits toLives, IntPredicate buff) throws IOException {
                boolean hit = false;
                if (ParArrJoinIndex.this.tuples<=0){
                    return false;
                }
                for (int from; (from = fromDocs.nextDoc()) != NO_MORE_DOCS; ) {
                    if (fromLives != null) {
                        if (!fromLives.get(from)) {
                            continue;
                        }
                    }
                    for (int toPos=0; toPos<ParArrJoinIndex.this.tuples; toPos++) {
                         if (from==ParArrJoinIndex.this.toDocs[toPos]) {
                             int toDoc = ParArrJoinIndex.this.fromDocs[toPos];
                             if (toLives!=null) {
                                 if (!toLives.get(toDoc)){
                                     break;
                                 }
                             }
                             hit|=buff.test(toDoc);
                             //break; no shotcut to may occurs again
                         }
                    }
                }
                return hit;
            }

            @Override
            public boolean isEmpty() {
                return ParArrJoinIndex.this.isEmpty();
            }

            @Override
            public JoinIndex reverse() {
                return ParArrJoinIndex.this;
            }
        };
    }
}
