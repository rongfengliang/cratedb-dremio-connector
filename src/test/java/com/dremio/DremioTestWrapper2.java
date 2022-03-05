package com.dremio;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.DremioGetObject;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.HyperVectorValueIterator;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.HyperVectorWrapper;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.ItemsSketch.Row;
import org.apache.datasketches.memory.Memory;
import org.joda.time.Period;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DremioTestWrapper2 extends DremioTestWrapper {
    static final Logger logger = LoggerFactory.getLogger(BaseTestQuery2.class);
    private static boolean VERBOSE_DEBUG = false;
    public static final int EXPECTED_BATCH_COUNT_NOT_SET = -1;
    public static final int MAX_SAMPLE_RECORDS_TO_PRINT_ON_FAILURE = 20;
    private TestBuilderSelf testBuilder;
    private Object query;
    private QueryType queryType;
    private QueryType baselineQueryType;
    private boolean ordered;
    private BufferAllocator allocator;
    private String baselineOptionSettingQueries;
    private String testOptionSettingQueries;
    private boolean highPerformanceComparison;
    private List<Map<String, Object>> baselineRecords;
    private static Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineValuesForTDigestMap;
    private static Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap;
    private int expectedNumBatches;
    private TestResult latestResult;

    public DremioTestWrapper2(TestBuilderSelf testBuilder, BufferAllocator allocator, Object query, QueryType queryType,
                              String baselineOptionSettingQueries, String testOptionSettingQueries, QueryType baselineQueryType,
                              boolean ordered, boolean highPerformanceComparison, List<Map<String, Object>> baselineRecords,
                              int expectedNumBatches, Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineValuesForTDigestMap,
                              Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap) {
        super(null,null,null,null,null,null,
                null,false,false,null,-1,
                null,null);
        this.testBuilder = testBuilder;
        this.allocator = allocator;
        this.query = query;
        this.queryType = queryType;
        this.baselineQueryType = baselineQueryType;
        this.ordered = ordered;
        this.baselineOptionSettingQueries = baselineOptionSettingQueries;
        this.testOptionSettingQueries = testOptionSettingQueries;
        this.highPerformanceComparison = highPerformanceComparison;
        this.baselineRecords = baselineRecords;
        this.expectedNumBatches = expectedNumBatches;
        DremioTestWrapper2.baselineValuesForTDigestMap = baselineValuesForTDigestMap;
        DremioTestWrapper2.baselineValuesForItemsSketchMap = baselineValuesForItemsSketchMap;
    }

    public TestResult run() throws Exception {
        if (this.testBuilder.getExpectedSchema() != null) {
            this.compareSchemaOnly();
        } else if (this.ordered) {
            this.compareOrderedResults();
        } else {
            this.compareUnorderedResults();
        }

        return this.latestResult;
    }

    private BufferAllocator getAllocator() {
        return this.allocator;
    }

    private void compareHyperVectors(Map<String, HyperVectorValueIterator> expectedRecords, Map<String, HyperVectorValueIterator> actualRecords) throws Exception {
        Iterator var3 = expectedRecords.keySet().iterator();

        while(var3.hasNext()) {
            String s = (String)var3.next();
            Assert.assertNotNull("Expected column '" + s + "' not found.", actualRecords.get(s));
            Assert.assertEquals(((HyperVectorValueIterator)expectedRecords.get(s)).getTotalRecords(), ((HyperVectorValueIterator)actualRecords.get(s)).getTotalRecords());
            HyperVectorValueIterator expectedValues = (HyperVectorValueIterator)expectedRecords.get(s);
            HyperVectorValueIterator actualValues = (HyperVectorValueIterator)actualRecords.get(s);

            for(int i = 0; expectedValues.hasNext(); ++i) {
                compareValuesErrorOnMismatch(expectedValues.next(), actualValues.next(), i, s);
            }
        }

        this.cleanupHyperValueIterators(expectedRecords.values());
        this.cleanupHyperValueIterators(actualRecords.values());
    }

    private void cleanupHyperValueIterators(Collection<HyperVectorValueIterator> hyperBatches) {
        Iterator var2 = hyperBatches.iterator();

        while(var2.hasNext()) {
            HyperVectorValueIterator hvi = (HyperVectorValueIterator)var2.next();
            ValueVector[] var4 = hvi.getHyperVector().getValueVectors();
            int var5 = var4.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                ValueVector vv = var4[var6];
                vv.clear();
            }
        }

    }

    public static void compareMergedVectors(Map<String, List<Object>> expectedRecords, Map<String, List<Object>> actualRecords) throws Exception {
        validateColumnSets(expectedRecords, actualRecords);
        Iterator var2 = actualRecords.keySet().iterator();

        while(var2.hasNext()) {
            String s = (String)var2.next();
            List<?> expectedValues = (List)expectedRecords.get(s);
            List<?> actualValues = (List)actualRecords.get(s);
            Assert.assertEquals(String.format("Incorrect number of rows returned by query.\nquery: %s\nexpected: %s\nactual: %s", s, expectedValues, actualValues), (long)expectedValues.size(), (long)actualValues.size());

            for(int i = 0; i < expectedValues.size(); ++i) {
                try {
                    compareValuesErrorOnMismatch(expectedValues.get(i), actualValues.get(i), i, s);
                } catch (Exception var8) {
                    throw new Exception(var8.getMessage() + "\n\n" + printNearbyRecords(expectedRecords, actualRecords, i), var8);
                }
            }
        }

        if (actualRecords.size() < expectedRecords.size()) {
            throw new Exception(findMissingColumns(expectedRecords.keySet(), actualRecords.keySet()));
        }
    }

    private static void validateColumnSets(Map<String, List<Object>> expectedRecords, Map<String, List<Object>> actualRecords) {
        Set<String> expectedKeys = expectedRecords.keySet();
        Set<String> actualKeys = actualRecords.keySet();
        if (!expectedKeys.equals(actualKeys)) {
            Assert.fail(String.format("Incorrect keys, expected:(%s) actual:(%s)", String.join(",", expectedKeys), String.join(",", actualKeys)));
        }

    }

    private static String printNearbyRecords(Map<String, List<Object>> expectedRecords, Map<String, List<Object>> actualRecords, int offset) {
        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        expected.append("Expected Records near verification failure:\n");
        actual.append("Actual Records near verification failure:\n");
        int firstRecordToPrint = Math.max(0, offset - 5);
        List<?> expectedValuesInFirstColumn = (List)expectedRecords.get(expectedRecords.keySet().iterator().next());
        List<?> actualValuesInFirstColumn = (List)expectedRecords.get(expectedRecords.keySet().iterator().next());
        int numberOfRecordsToPrint = Math.min(Math.min(20, expectedValuesInFirstColumn.size()), actualValuesInFirstColumn.size());

        for(int i = firstRecordToPrint; i < numberOfRecordsToPrint; ++i) {
            expected.append("Record Number: ").append(i).append(" { ");
            actual.append("Record Number: ").append(i).append(" { ");
            Iterator var10 = actualRecords.keySet().iterator();

            String s;
            List expectedValues;
            while(var10.hasNext()) {
                s = (String)var10.next();
                expectedValues = (List)actualRecords.get(s);
                actual.append(s).append(" : ").append(expectedValues.get(i)).append(",");
            }

            var10 = expectedRecords.keySet().iterator();

            while(var10.hasNext()) {
                s = (String)var10.next();
                expectedValues = (List)expectedRecords.get(s);
                expected.append(s).append(" : ").append(expectedValues.get(i)).append(",");
            }

            expected.append(" }\n");
            actual.append(" }\n");
        }

        return expected.append("\n\n").append(actual).toString();
    }

    private Map<String, HyperVectorValueIterator> addToHyperVectorMap(List<QueryDataBatch> records, RecordBatchLoader loader) throws SchemaChangeException, UnsupportedEncodingException {
        Map<String, HyperVectorValueIterator> combinedVectors = new TreeMap();
        long totalRecords = 0L;
        int size = records.size();

        for(int i = 0; i < size; ++i) {
            QueryDataBatch batch = (QueryDataBatch)records.get(i);
            loader.load(batch.getHeader().getDef(), batch.getData());
            logger.debug("reading batch with " + loader.getRecordCount() + " rows, total read so far " + totalRecords);
            totalRecords += (long)loader.getRecordCount();
            Iterator var9 = loader.iterator();

            while(var9.hasNext()) {
                VectorWrapper<?> w = (VectorWrapper)var9.next();
                String field = SchemaPath.getSimplePath(w.getField().getName()).toExpr();
                if (!combinedVectors.containsKey(field)) {
                    ValueVector[] vvList = (ValueVector[])Array.newInstance(TypeHelper.getValueVectorClass(w.getField()), 1);
                    vvList[0] = w.getValueVector();
                    combinedVectors.put(field, new HyperVectorValueIterator(w.getField(), new HyperVectorWrapper(w.getField(), vvList)));
                } else {
                    ((HyperVectorValueIterator)combinedVectors.get(field)).getHyperVector().addVector(w.getValueVector());
                }
            }
        }

        Iterator var13 = combinedVectors.values().iterator();

        while(var13.hasNext()) {
            HyperVectorValueIterator hvi = (HyperVectorValueIterator)var13.next();
            hvi.determineTotalSize();
        }

        return combinedVectors;
    }

    private static Object getVectorObject(ValueVector vector, int index) {
        return DremioGetObject.getObject(vector, index);
    }

    public static Map<String, List<Object>> addToCombinedVectorResults(Iterable<VectorAccessible> batches) throws SchemaChangeException, UnsupportedEncodingException {
        Map<String, List<Object>> combinedVectors = new TreeMap();
        long totalRecords = 0L;
        BatchSchema schema = null;
        Iterator var5 = batches.iterator();

        label92:
        while(var5.hasNext()) {
            VectorAccessible loader = (VectorAccessible)var5.next();
            Iterator var7;
            if (schema == null) {
                schema = loader.getSchema();
                var7 = schema.iterator();

                while(var7.hasNext()) {
                    Field mf = (Field)var7.next();
                    combinedVectors.put(SchemaPath.getSimplePath(mf.getName()).toExpr(), new ArrayList());
                }
            } else {
                schema = loader.getSchema();
            }

            logger.debug("reading batch with " + loader.getRecordCount() + " rows, total read so far " + totalRecords);
            totalRecords += (long)loader.getRecordCount();
            var7 = loader.iterator();

            while(true) {
                while(true) {
                    if (!var7.hasNext()) {
                        continue label92;
                    }

                    VectorWrapper<?> w = (VectorWrapper)var7.next();
                    String field = SchemaPath.getSimplePath(w.getField().getName()).toExpr();
                    ValueVector[] vectors;
                    if (w.isHyper()) {
                        vectors = w.getValueVectors();
                    } else {
                        vectors = new ValueVector[]{w.getValueVector()};
                    }

                    SelectionVector2 sv2 = null;
                    SelectionVector4 sv4 = null;
                    switch(schema.getSelectionVectorMode()) {
                        case TWO_BYTE:
                            sv2 = loader.getSelectionVector2();
                            break;
                        case FOUR_BYTE:
                            sv4 = loader.getSelectionVector4();
                    }

                    int complexIndex;
                    int batchIndex;
                    if (sv4 != null) {
                        for(int j = 0; j < sv4.getCount(); ++j) {
                            complexIndex = sv4.get(j);
                            batchIndex = complexIndex >> 16;
                            int recordIndexInBatch = complexIndex & '\uffff';
                            Object obj = getVectorObject(vectors[batchIndex], recordIndexInBatch);
                            if (obj != null && obj instanceof Text) {
                                obj = obj.toString();
                            }

                            ((List)combinedVectors.get(field)).add(obj);
                        }
                    } else {
                        ValueVector[] var13 = vectors;
                        complexIndex = vectors.length;

                        for(batchIndex = 0; batchIndex < complexIndex; ++batchIndex) {
                            ValueVector vv = var13[batchIndex];

                            for(int j = 0; j < loader.getRecordCount(); ++j) {
                                int index;
                                if (sv2 != null) {
                                    index = sv2.getIndex(j);
                                } else {
                                    index = j;
                                }

                                Object obj = getVectorObject(vv, index);
                                if (obj != null && obj instanceof Text) {
                                    obj = obj.toString();
                                }

                                ((List)combinedVectors.get(field)).add(obj);
                            }
                        }
                    }
                }
            }
        }

        return combinedVectors;
    }

    protected void compareSchemaOnly() throws Exception {
        RecordBatchLoader loader = new RecordBatchLoader(this.getAllocator());
        List actual = null;
        boolean var18 = false;

        try {
            var18 = true;
            actual = this.runQueryAndGetResults();
            QueryDataBatch batch = (QueryDataBatch)actual.get(0);
            loader.load(batch.getHeader().getDef(), batch.getData());
            BatchSchema schema = loader.getSchema();
            List<Pair<SchemaPath, MajorType>> expectedSchema = this.testBuilder.getExpectedSchema();
            if (schema.getFieldCount() != expectedSchema.size()) {
                throw new Exception(String.format("Expected and actual numbers of columns do not match. Expected: %s, Actual: %s.", expectedSchema, schema));
            }

            int i = 0;

            while(true) {
                if (i >= schema.getFieldCount()) {
                    var18 = false;
                    break;
                }

                String actualSchemaPath = schema.getColumn(i).getName();
                MinorType actualMinorType = Types.getMinorTypeForArrowType(schema.getColumn(i).getType());
                String expectedSchemaPath = ((SchemaPath)((Pair)expectedSchema.get(i)).getLeft()).getAsUnescapedPath();
                MinorType expectedMinorType = MajorTypeHelper.getArrowMinorType(((MajorType)((Pair)expectedSchema.get(i)).getValue()).getMinorType());
                if (!actualSchemaPath.equals(expectedSchemaPath) || !actualMinorType.equals(expectedMinorType)) {
                    throw new Exception(String.format("Schema path or type mismatch for column #%d:\nExpected schema path: %s\nActual   schema path: %s\nExpected type: %s\nActual   type: %s", i, expectedSchemaPath, actualSchemaPath, expectedMinorType, actualMinorType));
                }

                ++i;
            }
        } finally {
            if (var18) {
                if (actual != null) {
                    Iterator var12 = actual.iterator();

                    while(var12.hasNext()) {
                        QueryDataBatch batch = (QueryDataBatch)var12.next();

                        try {
                            batch.release();
                        } catch (Exception var19) {
                            logger.error("Failed to release query output batch");
                        }
                    }
                }

                loader.clear();
            }
        }

        if (actual != null) {
            Iterator var22 = actual.iterator();

            while(var22.hasNext()) {
                QueryDataBatch batch = (QueryDataBatch)var22.next();

                try {
                    batch.release();
                } catch (Exception var20) {
                    logger.error("Failed to release query output batch");
                }
            }
        }

        loader.clear();
    }

    protected void compareUnorderedResults() throws Exception {
        RecordBatchLoader loader = new RecordBatchLoader(this.getAllocator());
        List<QueryDataBatch> actual = Collections.emptyList();
        List<QueryDataBatch> expected = Collections.emptyList();
        List<Map<String, Object>> expectedRecords = new ArrayList();
        ArrayList actualRecords = new ArrayList();

        try {
            actual = this.runQueryAndGetResults();
            this.checkNumBatches(actual);
            this.addTypeInfoIfMissing((QueryDataBatch)actual.get(0), this.testBuilder);
            addToMaterializedResults(actualRecords, actual, loader);
            if (this.baselineRecords == null) {
                BaseTestQuery2.test(this.baselineOptionSettingQueries);
                expected = BaseTestQuery2.testRunAndReturn(this.baselineQueryType, this.testBuilder.getValidationQuery());
                addToMaterializedResults((List)expectedRecords, expected, loader);
            } else {
                expectedRecords = this.baselineRecords;
            }

            compareResults((List)expectedRecords, actualRecords);
        } finally {
            this.cleanupBatches(actual, expected);
        }

    }

    protected void compareOrderedResults() throws Exception {
        if (this.highPerformanceComparison) {
            if (this.baselineQueryType == null) {
                throw new Exception("Cannot do a high performance comparison without using a baseline file");
            }

            this.compareResultsHyperVector();
        } else {
            this.compareMergedOnHeapVectors();
        }

    }

    private List<QueryDataBatch> runQueryAndGetResults() throws Exception {
        BaseTestQuery2.test(this.testOptionSettingQueries);
        List<QueryDataBatch> actual = BaseTestQuery2.testRunAndReturn(this.queryType, this.query);
        this.latestResult = new TestResult(((QueryDataBatch)actual.get(0)).getHeader());
        return actual;
    }

    public void compareMergedOnHeapVectors() throws Exception {
        RecordBatchLoader loader = new RecordBatchLoader(this.getAllocator());
        BatchSchema schema = null;
        List<QueryDataBatch> actual = Collections.emptyList();
        List expected = Collections.emptyList();

        try {
            actual = this.runQueryAndGetResults();
            this.checkNumBatches(actual);
            this.addTypeInfoIfMissing((QueryDataBatch)actual.get(0), this.testBuilder);
            DremioTestWrapper2.BatchIterator batchIter = new DremioTestWrapper2.BatchIterator(actual, loader);
            Map<String, List<Object>> actualSuperVectors = addToCombinedVectorResults(batchIter);
            batchIter.close();
            Map expectedSuperVectors;
            if (this.baselineRecords == null) {
                BaseTestQuery2.test(this.baselineOptionSettingQueries);
                expected = BaseTestQuery2.testRunAndReturn(this.baselineQueryType, this.testBuilder.getValidationQuery());
                DremioTestWrapper2.BatchIterator exBatchIter = new DremioTestWrapper2.BatchIterator(expected, loader);
                expectedSuperVectors = addToCombinedVectorResults(exBatchIter);
                exBatchIter.close();
            } else {
                expectedSuperVectors = translateRecordListToHeapVectors(this.baselineRecords);
            }

            compareMergedVectors(expectedSuperVectors, actualSuperVectors);
        } catch (Exception var12) {
            throw new Exception(var12.getMessage() + "\nFor query: " + this.query, var12);
        } finally {
            this.cleanupBatches(expected, actual);
        }

    }

    public static Map<String, List<Object>> translateRecordListToHeapVectors(List<Map<String, Object>> records) {
        Map<String, List<Object>> ret = new TreeMap();
        Iterator var2 = ((Map)records.get(0)).keySet().iterator();

        while(var2.hasNext()) {
            String s = (String)var2.next();
            ret.put(s, new ArrayList());
        }

        var2 = records.iterator();

        while(var2.hasNext()) {
            Map<String, Object> m = (Map)var2.next();
            Iterator var4 = m.keySet().iterator();

            while(var4.hasNext()) {
                String s = (String)var4.next();
                ((List)ret.get(s)).add(m.get(s));
            }
        }

        return ret;
    }

    public void compareResultsHyperVector() throws Exception {
        RecordBatchLoader loader = new RecordBatchLoader(this.getAllocator());
        List<QueryDataBatch> results = this.runQueryAndGetResults();
        this.checkNumBatches(results);
        this.addTypeInfoIfMissing((QueryDataBatch)results.get(0), this.testBuilder);
        Map<String, HyperVectorValueIterator> actualSuperVectors = this.addToHyperVectorMap(results, loader);
        BaseTestQuery2.test(this.baselineOptionSettingQueries);
        List<QueryDataBatch> expected = BaseTestQuery2.testRunAndReturn(this.baselineQueryType, this.testBuilder.getValidationQuery());
        Map<String, HyperVectorValueIterator> expectedSuperVectors = this.addToHyperVectorMap(expected, loader);
        this.compareHyperVectors(expectedSuperVectors, actualSuperVectors);
        this.cleanupBatches(results, expected);
    }

    private void checkNumBatches(List<QueryDataBatch> results) {
        if (this.expectedNumBatches != -1) {
            int actualNumBatches = results.size();
            Assert.assertEquals(String.format("Expected %d batches but query returned %d non empty batch(es)%n", this.expectedNumBatches, actualNumBatches), (long)this.expectedNumBatches, (long)actualNumBatches);
        } else if (results.isEmpty() && !this.baselineRecords.isEmpty()) {
            Assert.fail("No records returned.");
        }

    }

    private void addTypeInfoIfMissing(QueryDataBatch batch, TestBuilderSelf testBuilder) {
        if (!testBuilder.typeInfoSet()) {
            Map<SchemaPath, MajorType> typeMap = this.getTypeMapFromBatch(batch);
            testBuilder.baselineTypes(typeMap);
        }

    }

    private Map<SchemaPath, MajorType> getTypeMapFromBatch(QueryDataBatch batch) {
        Map<SchemaPath, MajorType> typeMap = new TreeMap();

        for(int i = 0; i < batch.getHeader().getDef().getFieldCount(); ++i) {
            typeMap.put(SchemaPath.getSimplePath(SerializedFieldHelper.create(batch.getHeader().getDef().getField(i)).getName()), batch.getHeader().getDef().getField(i).getMajorType());
        }

        return typeMap;
    }

    @SafeVarargs
    private final void cleanupBatches(List<QueryDataBatch>... results) {
        List[] var2 = results;
        int var3 = results.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            List<QueryDataBatch> resultList = var2[var4];
            Iterator var6 = resultList.iterator();

            while(var6.hasNext()) {
                QueryDataBatch result = (QueryDataBatch)var6.next();
                result.release();
            }
        }

    }

    public static void addToMaterializedResults(List<Map<String, Object>> materializedRecords, List<QueryDataBatch> records, RecordBatchLoader loader) throws SchemaChangeException, UnsupportedEncodingException {
        long totalRecords = 0L;
        int size = records.size();

        for(int i = 0; i < size; ++i) {
            QueryDataBatch batch = (QueryDataBatch)records.get(0);
            loader.load(batch.getHeader().getDef(), batch.getData());
            logger.debug("reading batch with " + loader.getRecordCount() + " rows, total read so far " + totalRecords);
            totalRecords += (long)loader.getRecordCount();

            for(int j = 0; j < loader.getRecordCount(); ++j) {
                Map<String, Object> record = new LinkedHashMap();

                VectorWrapper w;
                Object obj;
                for(Iterator var10 = loader.iterator(); var10.hasNext(); record.put(SchemaPath.getSimplePath(w.getField().getName()).toExpr(), obj)) {
                    w = (VectorWrapper)var10.next();
                    obj = getVectorObject(w.getValueVector(), j);
                    if (obj != null) {
                        if (obj instanceof Text) {
                            obj = obj.toString();
                        }

                        record.put(SchemaPath.getSimplePath(w.getField().getName()).toExpr(), obj);
                    }
                }

                materializedRecords.add(record);
            }

            records.remove(0);
            batch.release();
            loader.clear();
        }

    }

    public static boolean compareValuesErrorOnMismatch(Object expected, Object actual, int counter, String column) throws Exception {
        if (compareValues(expected, actual, counter, column)) {
            return true;
        } else if (expected == null) {
            throw new Exception("at position " + counter + " column '" + column + "' mismatched values, expected: null but received " + actual + "(" + actual.getClass().getSimpleName() + ")");
        } else if (actual == null) {
            throw new Exception("unexpected null at position " + counter + " column '" + column + "' should have been:  " + expected);
        } else if (actual instanceof byte[]) {
            throw new Exception("at position " + counter + " column '" + column + "' mismatched values, expected: " + new String((byte[])expected, "UTF-8") + " but received " + new String((byte[])actual, "UTF-8"));
        } else if (!expected.equals(actual)) {
            throw new Exception(String.format("at position %d column '%s' mismatched values, \nexpected (%s):\n\n%s\n\nbut received (%s):\n\n%s\n\nHints:\n (1) Results are actually wrong\n (2) If results 'look right', then check if integer type is 'long' and not 'int' (so 1 is 1L)\n", counter, column, expected.getClass().getSimpleName(), expected, actual.getClass().getSimpleName(), actual));
        } else {
            return true;
        }
    }

    public static boolean compareValues(Object expected, Object actual, int counter, String column) throws Exception {
        if (expected == null) {
            if (actual == null) {
                if (VERBOSE_DEBUG) {
                    logger.debug("(1) at position " + counter + " column '" + column + "' matched value:  " + expected);
                }

                return true;
            } else {
                return false;
            }
        } else if (actual == null) {
            return false;
        } else if (baselineValuesForTDigestMap != null && baselineValuesForTDigestMap.get(column) != null) {
            if (!approximatelyEqualFromTDigest(expected, actual, (DremioTestWrapper.BaselineValuesForTDigest)baselineValuesForTDigestMap.get(column))) {
                return false;
            } else {
                if (VERBOSE_DEBUG) {
                    logger.debug("at position " + counter + " column '" + column + "' matched value:  " + expected);
                }

                return true;
            }
        } else if (baselineValuesForItemsSketchMap != null && baselineValuesForItemsSketchMap.get(column) != null) {
            if (!verifyItemsSketchValues(actual, (DremioTestWrapper.BaselineValuesForItemsSketch)baselineValuesForItemsSketchMap.get(column))) {
                return false;
            } else {
                if (VERBOSE_DEBUG) {
                    logger.debug("at position " + counter + " column '" + column + "' matched value:  " + expected);
                }

                return true;
            }
        } else if (actual instanceof byte[]) {
            if (!Arrays.equals((byte[])expected, (byte[])actual)) {
                return false;
            } else {
                if (VERBOSE_DEBUG) {
                    logger.debug("at position " + counter + " column '" + column + "' matched value " + new String((byte[])expected, "UTF-8"));
                }

                return true;
            }
        } else if (actual instanceof Double && expected instanceof Double) {
            double actualValue = (Double)actual;
            double expectedValue = (Double)expected;
            if (!actual.equals(expected)) {
                if (actualValue == 0.0D) {
                    if (Math.abs(actualValue - expectedValue) < 1.0E-7D) {
                        return true;
                    }
                } else if (Math.abs((actualValue - expectedValue) / actualValue) < 1.0E-7D) {
                    return true;
                }

                return false;
            } else {
                return true;
            }
        } else if (actual instanceof Float && expected instanceof Float) {
            float actualValue = (Float)actual;
            float expectedValue = (Float)expected;
            if (!actual.equals(expected)) {
                if (actualValue == 0.0F) {
                    if ((double)Math.abs(actualValue - expectedValue) < 1.0E-7D) {
                        return true;
                    }
                } else if (Math.abs((double)(actualValue - expectedValue) / (double)actualValue) < 1.0E-7D) {
                    return true;
                }

                return false;
            } else {
                return true;
            }
        } else if (actual instanceof Period && expected instanceof Period) {
            Period actualValue = ((Period)actual).normalizedStandard();
            Period expectedValue = ((Period)expected).normalizedStandard();
            return actualValue.equals(expectedValue);
        } else if (actual instanceof BigDecimal && expected instanceof BigDecimal) {
            return ((BigDecimal)actual).compareTo((BigDecimal)expected) == 0;
        } else if (!expected.equals(actual)) {
            return false;
        } else {
            if (VERBOSE_DEBUG) {
                logger.debug("at position " + counter + " column '" + column + "' matched value:  " + expected);
            }

            return true;
        }
    }

    private static boolean approximatelyEqualFromTDigest(Object expected, Object actual, DremioTestWrapper.BaselineValuesForTDigest value) {
        if (expected instanceof Double) {
            if (!(actual instanceof byte[])) {
                return false;
            } else {
                ByteBuffer buffer = ByteBuffer.wrap((byte[])actual);
                TDigest tDigest = MergingDigest.fromBytes(buffer);
                double out = tDigest.quantile(value.quartile);
                return Math.abs(((Double)expected - Double.valueOf(out)) / (Double)expected) <= value.tolerance;
            }
        } else {
            return false;
        }
    }

    private static boolean verifyItemsSketchValues(Object actual, DremioTestWrapper.BaselineValuesForItemsSketch value) {
        if (!(actual instanceof byte[])) {
            return false;
        } else {
            ItemsSketch sketch = ItemsSketch.getInstance(Memory.wrap((byte[])actual), value.serde);
            if (value.heavyHitters != null) {
                int size = value.heavyHitters.size();
                Row[] rows1 = sketch.getFrequentItems(5L, ErrorType.NO_FALSE_NEGATIVES);

                for(int i = 0; i < rows1.length; ++i) {
                    if (!rows1[i].getItem().equals(value.heavyHitters.get(i))) {
                        return false;
                    }
                }

                Row[] rows2 = sketch.getFrequentItems((long)size, ErrorType.NO_FALSE_NEGATIVES);

                for(int i = 0; i < rows2.length; ++i) {
                    if (!rows2[i].getItem().equals(value.heavyHitters.get(i))) {
                        return false;
                    }
                }
            }

            if (value.counts != null) {
                Iterator var7 = value.counts.iterator();
                if (var7.hasNext()) {
                    Pair<Object, Long> count = (Pair)var7.next();
                    Object key = count.getLeft();
                    Long val = (Long)count.getRight();
                    return (double)Math.abs(sketch.getEstimate(key) - val) / (double)val <= 0.01D;
                }
            }

            return true;
        }
    }

    public static void compareResults(List<Map<String, Object>> expectedRecords, List<Map<String, Object>> actualRecords) throws Exception {
        if (expectedRecords.size() != actualRecords.size()) {
            String expectedRecordExamples = serializeRecordExamplesToString(expectedRecords);
            String actualRecordExamples = serializeRecordExamplesToString(actualRecords);
            throw new AssertionError(String.format("Different number of records returned - expected:<%d> but was:<%d>\n\nSome examples of expected records:\n%s\n\n Some examples of records returned by the test query:\n%s", expectedRecords.size(), actualRecords.size(), expectedRecordExamples, actualRecordExamples));
        } else {
            int counter = 0;

            for(Iterator var5 = expectedRecords.iterator(); var5.hasNext(); ++counter) {
                Map<String, Object> expectedRecord = (Map)var5.next();
                int i = 0;
                boolean found = false;

                label43:
                for(Iterator var7 = actualRecords.iterator(); var7.hasNext(); ++i) {
                    Map<String, Object> actualRecord = (Map)var7.next();
                    Iterator var9 = actualRecord.keySet().iterator();

                    String s;
                    do {
                        if (!var9.hasNext()) {
                            if (actualRecord.size() < expectedRecord.size()) {
                                throw new Exception(findMissingColumns(expectedRecord.keySet(), actualRecord.keySet()));
                            }

                            found = true;
                            break label43;
                        }

                        s = (String)var9.next();
                        if (!expectedRecord.containsKey(s)) {
                            throw new AssertionError("Unexpected column '" + s + "' returned by query.\ngot: " + actualRecord.keySet() + "\nexpected: " + expectedRecord.keySet());
                        }
                    } while(compareValues(expectedRecord.get(s), actualRecord.get(s), counter, s));
                }

                if (!found) {
                    String expectedRecordExamples = serializeRecordExamplesToString(expectedRecords);
                    String actualRecordExamples = serializeRecordExamplesToString(actualRecords);
                    throw new Exception(String.format("After matching %d records, did not find expected record in result set:\n %s\n\nSome examples of expected records:\n%s\n\n Some examples of records returned by the test query:\n%s", counter, printRecord(expectedRecord), expectedRecordExamples, actualRecordExamples));
                }

                actualRecords.remove(i);
            }

            Assert.assertEquals(0L, (long)actualRecords.size());
        }
    }

    private static String serializeRecordExamplesToString(List<Map<String, Object>> records) {
        StringBuilder sb = new StringBuilder();

        for(int recordDisplayCount = 0; recordDisplayCount < 20 && recordDisplayCount < records.size(); ++recordDisplayCount) {
            sb.append(printRecord((Map)records.get(recordDisplayCount)));
        }

        return sb.toString();
    }

    private static String findMissingColumns(Set<String> expected, Set<String> actual) {
        String missingCols = "";
        Iterator var3 = expected.iterator();

        while(var3.hasNext()) {
            String colName = (String)var3.next();
            if (!actual.contains(colName)) {
                missingCols = missingCols + colName + ", ";
            }
        }

        return "Expected column(s) " + missingCols + " not found in result set: " + actual + ".";
    }

    private static String printRecord(Map<String, ?> record) {
        String ret = "";

        String s;
        for(Iterator var2 = record.keySet().iterator(); var2.hasNext(); ret = ret + s + " : " + record.get(s) + ", ") {
            s = (String)var2.next();
        }

        return ret + "\n";
    }

    public static class BaselineValuesForItemsSketch {
        public List<Pair<Object, Long>> counts = null;
        public List<Object> heavyHitters = null;
        public ArrayOfItemsSerDe serde;

        public BaselineValuesForItemsSketch(List<Pair<Object, Long>> counts, List<Object> heavyHitters, ArrayOfItemsSerDe serde) {
            this.counts = counts;
            this.heavyHitters = heavyHitters;
            this.serde = serde;
        }
    }

    public static class BaselineValuesForTDigest {
        public double tolerance;
        public double quartile;

        public BaselineValuesForTDigest(double tolerance, double quartile) {
            this.tolerance = tolerance;
            this.quartile = quartile;
        }
    }

    private static class BatchIterator implements Iterable<VectorAccessible>, AutoCloseable {
        private final List<QueryDataBatch> dataBatches;
        private final RecordBatchLoader batchLoader;

        public BatchIterator(List<QueryDataBatch> dataBatches, RecordBatchLoader batchLoader) {
            this.dataBatches = dataBatches;
            this.batchLoader = batchLoader;
        }

        public Iterator<VectorAccessible> iterator() {
            return new Iterator<VectorAccessible>() {
                int index = -1;

                public boolean hasNext() {
                    return this.index < BatchIterator.this.dataBatches.size() - 1;
                }

                public VectorAccessible next() {
                    ++this.index;
                    if (this.index == BatchIterator.this.dataBatches.size()) {
                        throw new RuntimeException("Tried to call next when iterator had no more items.");
                    } else {
                        BatchIterator.this.batchLoader.clear();
                        QueryDataBatch batch = (QueryDataBatch)BatchIterator.this.dataBatches.get(this.index);

                        try {
                            BatchIterator.this.batchLoader.load(batch.getHeader().getDef(), batch.getData());
                        } catch (SchemaChangeException var3) {
                            throw new RuntimeException(var3);
                        }

                        return BatchIterator.this.batchLoader;
                    }
                }

                public void remove() {
                    throw new UnsupportedOperationException("Removing is not supported");
                }
            };
        }

        public void close() throws Exception {
            this.batchLoader.clear();
        }
    }
}

