package org.apache.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.codecs.lucene90.Lucene90PointsWriter;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class TestPointsJoinIndex extends LuceneTestCase {
    @Test
    public void testSomething() throws IOException {
        try(Directory dir = newFSDirectory(createTempDir("justSoYouGetSomeChannelErrors"))) {

            Codec codec = new Lucene95Codec();//getCodec();

            SegmentInfo segmentInfo =
                    new SegmentInfo(
                            dir,
                            Version.LATEST,
                            Version.LATEST,
                            "_0",
                            1,
                            false,
                            codec,
                            Collections.emptyMap(),
                            StringHelper.randomId(),
                            Collections.emptyMap(),
                            null);
            //FieldInfo proto = oneDocReader.getFieldInfos().fieldInfo("field");
            FieldInfo field =
                    new FieldInfo(
                            "stub",
                            0,
                            false,
                            false,
                            false,
                            IndexOptions.NONE,
                            DocValuesType.NONE,
                            -1,
                            new HashMap<>(),
                            2,
                            2,
                            4,
                            0,
                            VectorEncoding.BYTE,
                            VectorSimilarityFunction.COSINE,
                            false);

            FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] {field});

            SegmentWriteState writeState =
                    new SegmentWriteState(
                            null, dir, segmentInfo, fieldInfos, null, new IOContext(new FlushInfo(1, 20)));
            try (Lucene90PointsWriter writer = new Lucene90PointsWriter(writeState)) {
                PointValuesWriter pointValuesWriter = new PointValuesWriter(Counter.newCounter(), field);
                BytesRef bytesRef = new BytesRef(new byte[Integer.BYTES * 2]);
                for(int i=0;i<10;i++) {
                    IntPoint.encodeDimension(i, bytesRef.bytes, 0);
                    IntPoint.encodeDimension(10-i, bytesRef.bytes, 4);
                    pointValuesWriter.addPackedValue(0, bytesRef);
                }
                pointValuesWriter.flush(writeState, null, writer);
                writer.finish();
            }

            SegmentReadState readerState = new SegmentReadState(dir, segmentInfo,fieldInfos, IOContext.READ );
            try(Lucene90PointsReader reader = new Lucene90PointsReader(readerState)){
                PointValues pointValues = reader.getValues("stub");//.intersect();
                pointValues.intersect(dumpVisitor(0,0,Integer.MAX_VALUE,5));
// PointValues values = reader.getPointValues(field);
            }
        }
    }

    public static PointValues.IntersectVisitor dumpVisitor(int x0, int y0, int x1, int y1) {
        byte[] x0b = new byte[Integer.BYTES];
        IntPoint.encodeDimension(x0, x0b, 0);
        byte[] y0b = new byte[Integer.BYTES];
        IntPoint.encodeDimension(y0, y0b, 0);
        byte[] x1b = new byte[Integer.BYTES];
        IntPoint.encodeDimension(x1, x1b, 0);
        byte[] y1b = new byte[Integer.BYTES];
        IntPoint.encodeDimension(y1, y1b, 0);
        BytesRef lowerPoint = pack(x0b, y0b);
        BytesRef upperPoint = pack(x1b, y1b);
        int bytesPerDim = Integer.BYTES;
        int numDims = 2;
        return new PointValues.IntersectVisitor() {

            private final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);

            private boolean matches(byte[] packedValue) {

                for (int dim = 0; dim < numDims; dim++) {
                    int offset = dim * bytesPerDim;
                    if (comparator.compare(packedValue, offset, lowerPoint.bytes, offset) < 0) {
                        // Doc's value is too low, in this dimension
                        return false;
                    }
                    if (comparator.compare(packedValue, offset, upperPoint.bytes, offset) > 0) {
                        // Doc's value is too high, in this dimension
                        return false;
                    }
                }
                return true;
            }

            private PointValues.Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {

                boolean crosses = false;

                for (int dim = 0; dim < numDims; dim++) {
                    int offset = dim * bytesPerDim;

                    if (comparator.compare(minPackedValue, offset, upperPoint.bytes, offset) > 0
                            || comparator.compare(maxPackedValue, offset, lowerPoint.bytes, offset) < 0) {
                        return PointValues.Relation.CELL_OUTSIDE_QUERY;
                    }

                    crosses |=
                            comparator.compare(minPackedValue, offset, lowerPoint.bytes, offset) < 0
                                    || comparator.compare(maxPackedValue, offset, upperPoint.bytes, offset) > 0;
                }

                if (crosses) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                } else {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
            }

            //DocIdSetBuilder.BulkAdder adder;

            @Override
            public void grow(int count) {
                System.out.println("grow "+count);
                //adder = result.grow(count);
            }

            @Override
            public void visit(int docID) {
                //System.out.println("visit doc "+docID);
                throw new UnsupportedOperationException("never dump whole leaf");
                //adder.add(docID);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
                throw new UnsupportedOperationException("never dump whole leaf");
//                int doc;
//                System.out.print("visit docs ");
//                while ((doc=iterator.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS){
//                    System.out.print(doc);
//                    System.out.print(", ");
//                }
//                System.out.println();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (matches(packedValue)) {
                    int [] vertex = unpackInts(packedValue);
                    System.out.println("visit doc "+ Arrays.toString(vertex));
                    //adder.add(docID);
                }
            }

            private int[] unpackInts(byte[] packedValue) {
                int[] point = new int[packedValue.length / Integer.BYTES];
                for (int offset = 0, i=0; offset<packedValue.length; offset+=Integer.BYTES, i++) {
                    point[i]=IntPoint.decodeDimension(packedValue, offset);
                }
                assert packedValue.length%Integer.BYTES==0;
                return point;
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                if (matches(packedValue)) {
                    int doc;
                    System.out.print("visit docs ");
                    while ((doc= iterator.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS){
                        System.out.print(Arrays.toString(unpackInts(packedValue)));
                        System.out.print(", ");
                    }
                    System.out.println();
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return relate(minPackedValue, maxPackedValue);
            }
        };
    }


    /** BinaryPoint.pack*/
    private static BytesRef pack(byte[]... point) {
        if (point == null) {
            throw new IllegalArgumentException("point must not be null");
        }
        if (point.length == 0) {
            throw new IllegalArgumentException("point must not be 0 dimensions");
        }
        if (point.length == 1) {
            return new BytesRef(point[0]);
        }
        int bytesPerDim = -1;
        for (byte[] dim : point) {
            if (dim == null) {
                throw new IllegalArgumentException("point must not have null values");
            }
            if (bytesPerDim == -1) {
                if (dim.length == 0) {
                    throw new IllegalArgumentException("point must not have 0-length values");
                }
                bytesPerDim = dim.length;
            } else if (dim.length != bytesPerDim) {
                throw new IllegalArgumentException(
                        "all dimensions must have same bytes length; got "
                                + bytesPerDim
                                + " and "
                                + dim.length);
            }
        }
        byte[] packed = new byte[bytesPerDim * point.length];
        for (int i = 0; i < point.length; i++) {
            System.arraycopy(point[i], 0, packed, i * bytesPerDim, bytesPerDim);
        }
        return new BytesRef(packed);
    }
}
