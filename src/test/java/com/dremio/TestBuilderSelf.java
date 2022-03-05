package com.dremio;


import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.parser.ExprLexer;
import com.dremio.common.expression.parser.ExprParser;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;

public class TestBuilderSelf {
    
    protected Object query;
    private UserBitShared.QueryType queryType;
    private Boolean ordered;
    private boolean approximateEquality;
    private BufferAllocator allocator;
    protected Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap;
    private String baselineOptionSettingQueries;
    private String testOptionSettingQueries;
    private boolean highPerformanceComparison;
    protected String[] baselineColumns;
    private List<Map<String, Object>> baselineRecords;
    private int expectedNumBatches;
    private Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineValuesForTDigestMap;
    private Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap;

    public TestBuilderSelf(BufferAllocator allocator) {
        this.expectedNumBatches = -1;
        this.allocator = allocator;
        this.reset();
    }

    public TestBuilderSelf(BufferAllocator allocator, Object query, UserBitShared.QueryType queryType, Boolean ordered, boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap, String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison, int expectedNumBatches, Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineValuesForTDigestMap, Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap) {
        this(allocator);
        if (ordered == null) {
            throw new RuntimeException("Ordering not set, when using a baseline file or query you must explicitly call the ordered() or unOrdered() method on the " + this.getClass().getSimpleName());
        } else {
            this.query = query;
            this.queryType = queryType;
            this.ordered = ordered;
            this.approximateEquality = approximateEquality;
            this.baselineTypeMap = baselineTypeMap;
            this.baselineOptionSettingQueries = baselineOptionSettingQueries;
            this.testOptionSettingQueries = testOptionSettingQueries;
            this.highPerformanceComparison = highPerformanceComparison;
            this.expectedNumBatches = expectedNumBatches;
            this.baselineValuesForTDigestMap = baselineValuesForTDigestMap;
            this.baselineValuesForItemsSketchMap = baselineValuesForItemsSketchMap;
        }
    }

    protected TestBuilderSelf reset() {
        this.query = "";
        this.ordered = null;
        this.approximateEquality = false;
        this.highPerformanceComparison = false;
        this.testOptionSettingQueries = "";
        this.baselineOptionSettingQueries = "";
        this.baselineRecords = null;
        this.baselineValuesForTDigestMap = null;
        this.baselineValuesForItemsSketchMap = null;
        return this;
    }

    public DremioTestWrapper build() throws Exception {
        if (!this.ordered && this.highPerformanceComparison) {
            throw new Exception("High performance comparison only available for ordered checks, to enforce this restriction, ordered() must be called first.");
        } else {
            return new DremioTestWrapper2(this, this.allocator, this.query, this.queryType, this.baselineOptionSettingQueries, this.testOptionSettingQueries, this.getValidationQueryType(), this.ordered, this.highPerformanceComparison, this.baselineRecords, this.expectedNumBatches, this.baselineValuesForTDigestMap, this.baselineValuesForItemsSketchMap);
        }
    }

    public List<Pair<SchemaPath, TypeProtos.MajorType>> getExpectedSchema() {
        return null;
    }

    public TestResult go() throws Exception {
        return this.build().run();
    }

    public TestBuilderSelf sqlQuery(String query) {
        this.query = QueryTestUtil.normalizeQuery(query);
        this.queryType = UserBitShared.QueryType.SQL;
        return this;
    }

    public TestBuilderSelf sqlQuery(String query, Object... replacements) {
        return this.sqlQuery(String.format(query, replacements));
    }

    public TestBuilderSelf preparedStatement(UserProtos.PreparedStatementHandle preparedStatementHandle) {
        this.queryType = UserBitShared.QueryType.PREPARED_STATEMENT;
        this.query = preparedStatementHandle;
        return this;
    }

    public TestBuilderSelf sqlQueryFromFile(String queryFile) throws IOException {
        String query = BaseTestQuery.getFile(queryFile);
        this.query = query;
        this.queryType = UserBitShared.QueryType.SQL;
        return this;
    }

    public TestBuilderSelf physicalPlanFromFile(String queryFile) throws IOException {
        String query = BaseTestQuery.getFile(queryFile);
        this.query = query;
        this.queryType = UserBitShared.QueryType.PHYSICAL;
        return this;
    }

    public TestBuilderSelf ordered() {
        this.ordered = true;
        return this;
    }

    public TestBuilderSelf unOrdered() {
        this.ordered = false;
        return this;
    }

    public TestBuilderSelf highPerformanceComparison() throws Exception {
        this.highPerformanceComparison = true;
        return this;
    }

    public TestBuilderSelf optionSettingQueriesForBaseline(String queries) {
        this.baselineOptionSettingQueries = queries;
        return this;
    }

    public TestBuilderSelf optionSettingQueriesForBaseline(String queries, Object... args) {
        this.baselineOptionSettingQueries = String.format(queries, args);
        return this;
    }

    public TestBuilderSelf optionSettingQueriesForTestQuery(String queries) {
        this.testOptionSettingQueries = queries;
        return this;
    }

    public TestBuilderSelf optionSettingQueriesForTestQuery(String query, Object... args) throws Exception {
        this.testOptionSettingQueries = String.format(query, args);
        return this;
    }

    public TestBuilderSelf approximateEquality() {
        this.approximateEquality = true;
        return this;
    }

    public static SchemaPath parsePath(String path) {
        try {
            ExprLexer lexer = new ExprLexer(new ANTLRStringStream(path));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            ExprParser parser = new ExprParser(tokens);
            ExprParser.parse_return ret = parser.parse();
            if (ret.e instanceof SchemaPath) {
                return (SchemaPath)ret.e;
            } else {
                throw new IllegalStateException("Schema path is not a valid format.");
            }
        } catch (RecognitionException var5) {
            throw new RuntimeException(var5);
        }
    }

    Object getValidationQuery() throws Exception {
        throw new RuntimeException("Must provide some kind of baseline, either a baseline file or another query");
    }

    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
        if (this.singleExplicitBaselineRecord()) {
            return null;
        } else {
            throw new RuntimeException("Must provide some kind of baseline, either a baseline file or another query");
        }
    }

    public TestBuilderSelf.JSONTestBuilder jsonBaselineFile(String filePath) {
        return new TestBuilderSelf.JSONTestBuilder(filePath, this.allocator, this.query, this.queryType, this.ordered, this.approximateEquality, this.baselineTypeMap, this.baselineOptionSettingQueries, this.testOptionSettingQueries, this.highPerformanceComparison, this.expectedNumBatches, this.baselineValuesForTDigestMap, this.baselineValuesForItemsSketchMap);
    }

    public TestBuilderSelf.CSVTestBuilder csvBaselineFile(String filePath) {
        return new TestBuilderSelf.CSVTestBuilder(filePath, this.allocator, this.query, this.queryType, this.ordered, this.approximateEquality, this.baselineTypeMap, this.baselineOptionSettingQueries, this.testOptionSettingQueries, this.highPerformanceComparison, this.expectedNumBatches, this.baselineValuesForTDigestMap, this.baselineValuesForItemsSketchMap);
    }

    public TestBuilderSelf.SchemaTestBuilder schemaBaseLine(List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema) {
        assert expectedSchema != null : "The expected schema can be provided once";

        assert this.baselineColumns == null : "The column information should be captured in expected schema, not baselineColumns";

        return new TestBuilderSelf.SchemaTestBuilder(this.allocator, this.query, this.queryType, this.baselineOptionSettingQueries, this.testOptionSettingQueries, expectedSchema);
    }

    public TestBuilderSelf baselineTypes(Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap) {
        this.baselineTypeMap = baselineTypeMap;
        return this;
    }

    boolean typeInfoSet() {
        return this.baselineTypeMap != null;
    }

    public TestBuilderSelf expectsEmptyResultSet() {
        this.unOrdered();
        this.baselineRecords = new ArrayList();
        return this;
    }

    public TestBuilderSelf expectsNumBatches(int expectedNumBatches) {
        this.expectedNumBatches = expectedNumBatches;
        return this;
    }

    public TestBuilderSelf baselineValues(Object... baselineValues) {
        assert this.getExpectedSchema() == null : "The expected schema is not needed when baselineValues are provided ";

        if (this.ordered == null) {
            throw new RuntimeException("Ordering not set, before specifying baseline data you must explicitly call the ordered() or unOrdered() method on the " + this.getClass().getSimpleName());
        } else {
            if (this.baselineRecords == null) {
                this.baselineRecords = new ArrayList();
            }

            Map<String, Object> ret = new LinkedHashMap();
            int i = 0;
            Assert.assertTrue("Must set expected columns before baseline values/records.", this.baselineColumns != null);
            if (baselineValues == null) {
                baselineValues = new Object[]{null};
            }

            Assert.assertEquals("Must supply the same number of baseline values as columns.", (long)baselineValues.length, (long)this.baselineColumns.length);
            String[] var4 = this.baselineColumns;
            int var5 = var4.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                String s = var4[var6];
                ret.put(s, baselineValues[i]);
                ++i;
            }

            this.baselineRecords.add(ret);
            return this;
        }
    }

    public TestBuilderSelf baselineTolerancesForTDigest(Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineValuesForTDigestMap) {
        this.baselineValuesForTDigestMap = baselineValuesForTDigestMap;
        return this;
    }

    public TestBuilderSelf baselineTolerancesForItemsSketch(Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap) {
        this.baselineValuesForItemsSketchMap = baselineValuesForItemsSketchMap;
        return this;
    }

    public TestBuilderSelf baselineRecords(List<Map<String, Object>> materializedRecords) {
        this.baselineRecords = materializedRecords;
        return this;
    }

    public TestBuilderSelf baselineColumns(String... columns) {
        assert this.getExpectedSchema() == null : "The expected schema is not needed when baselineColumns are provided ";

        this.baselineColumns = new String[columns.length];

        for(int i = 0; i < columns.length; ++i) {
            this.baselineColumns[i] = parsePath(columns[i]).toExpr();
        }

        return this;
    }

    private boolean singleExplicitBaselineRecord() {
        return this.baselineRecords != null;
    }

    public TestBuilderSelf.BaselineQueryTestBuilder sqlBaselineQuery(Object baselineQuery) {
        return new TestBuilderSelf.BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.SQL, this.allocator, this.query, this.queryType, this.ordered, this.approximateEquality, this.baselineTypeMap, this.baselineOptionSettingQueries, this.testOptionSettingQueries, this.highPerformanceComparison, this.expectedNumBatches, this.baselineValuesForTDigestMap, this.baselineValuesForItemsSketchMap);
    }

    public TestBuilderSelf.BaselineQueryTestBuilder sqlBaselineQuery(String query, String... replacements) {
        return this.sqlBaselineQuery(String.format(query, replacements));
    }

    public TestBuilderSelf.BaselineQueryTestBuilder sqlBaselineQueryFromFile(String baselineQueryFilename) throws IOException {
        String baselineQuery = BaseTestQuery.getFile(baselineQueryFilename);
        return new TestBuilderSelf.BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.SQL, this.allocator, this.query, this.queryType, this.ordered, this.approximateEquality, this.baselineTypeMap, this.baselineOptionSettingQueries, this.testOptionSettingQueries, this.highPerformanceComparison, this.expectedNumBatches, this.baselineValuesForTDigestMap, this.baselineValuesForItemsSketchMap);
    }

    public TestBuilderSelf.BaselineQueryTestBuilder physicalPlanBaselineQueryFromFile(String baselinePhysicalPlanPath) throws IOException {
        String baselineQuery = BaseTestQuery.getFile(baselinePhysicalPlanPath);
        return new TestBuilderSelf.BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.PHYSICAL, this.allocator, this.query, this.queryType, this.ordered, this.approximateEquality, this.baselineTypeMap, this.baselineOptionSettingQueries, this.testOptionSettingQueries, this.highPerformanceComparison, this.expectedNumBatches, this.baselineValuesForTDigestMap, this.baselineValuesForItemsSketchMap);
    }

    private String getDecimalPrecisionScaleInfo(TypeProtos.MajorType type) {
        String precision = "";
        switch(type.getMinorType()) {
            case DECIMAL18:
            case DECIMAL28SPARSE:
            case DECIMAL38SPARSE:
            case DECIMAL38DENSE:
            case DECIMAL28DENSE:
            case DECIMAL9:
                precision = String.format("(%d,%d)", type.getPrecision(), type.getScale());
            default:
                return precision;
        }
    }

    public static JsonStringArrayList<Object> listOf(Object... values) {
        JsonStringArrayList<Object> list = new JsonStringArrayList();
        Object[] var2 = values;
        int var3 = values.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            Object value = var2[var4];
            if (value instanceof CharSequence) {
                list.add(new Text(value.toString()));
            } else {
                list.add(value);
            }
        }

        return list;
    }

    public static JsonStringHashMap<String, Object> mapOf(Object... keyValueSequence) {
        Preconditions.checkArgument(keyValueSequence.length % 2 == 0, "Length of key value sequence must be even");
        JsonStringHashMap<String, Object> map = new JsonStringHashMap();

        for(int i = 0; i < keyValueSequence.length; i += 2) {
            Object value = keyValueSequence[i + 1];
            if (value instanceof CharSequence) {
                value = new Text(value.toString());
            }

            map.put((String)keyValueSequence[i], value);
        }

        return map;
    }

    public static String getNameOfMinorType(TypeProtos.MinorType type) {
        switch(type) {
            case DECIMAL18:
                return "decimal";
            case DECIMAL28SPARSE:
                return "decimal";
            case DECIMAL38SPARSE:
                return "decimal";
            case DECIMAL38DENSE:
            case DECIMAL28DENSE:
            default:
                throw new AssertionError("Unrecognized type " + type);
            case DECIMAL9:
                return "decimal";
            case BIT:
                return "bool";
            case TINYINT:
                return "tinyint";
            case UINT1:
                return "uint1";
            case SMALLINT:
                return "smallint";
            case UINT2:
                return "uint2";
            case INT:
                return "int";
            case UINT4:
                return "uint4";
            case BIGINT:
                return "bigint";
            case UINT8:
                return "uint8";
            case FLOAT4:
                return "float";
            case FLOAT8:
                return "double";
            case VARCHAR:
                return "varchar";
            case VAR16CHAR:
                return "utf16";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case TIMESTAMP:
                return "timestamp";
            case VARBINARY:
                return "binary";
            case LATE:
                throw new AssertionError("The late type should never appear in execution or an SQL query, so it does not have a name to refer to it.");
        }
    }

    public class BaselineQueryTestBuilder extends TestBuilderSelf {
        private Object baselineQuery;
        private UserBitShared.QueryType baselineQueryType;

        BaselineQueryTestBuilder(Object baselineQuery, UserBitShared.QueryType baselineQueryType, BufferAllocator allocator, Object query, UserBitShared.QueryType queryType, Boolean ordered, boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap, String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison, int expectedNumBatches, Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineTolerances, Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap) {
            super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison, expectedNumBatches, baselineTolerances, baselineValuesForItemsSketchMap);
            this.baselineQuery = baselineQuery;
            this.baselineQueryType = baselineQueryType;
        }

        Object getValidationQuery() {
            return this.baselineQuery;
        }

        protected UserBitShared.QueryType getValidationQueryType() throws Exception {
            return this.baselineQueryType;
        }

        boolean typeInfoSet() {
            return true;
        }
    }

    public class JSONTestBuilder extends TestBuilderSelf {
        private String baselineFilePath;

        JSONTestBuilder(String baselineFile, BufferAllocator allocator, Object query, UserBitShared.QueryType queryType, Boolean ordered, boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap, String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison, int expectedNumBatches, Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineTolerances, Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap) {
            super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison, expectedNumBatches, baselineTolerances, baselineValuesForItemsSketchMap);
            this.baselineFilePath = baselineFile;
            this.baselineColumns = new String[]{"*"};
        }

        String getValidationQuery() {
            return "select " + Joiner.on(", ").join(Iterables.transform(Arrays.asList(this.baselineColumns), (column) -> {
                return column.replace('`', '"');
            })) + " from cp.\"" + this.baselineFilePath + "\"";
        }

        protected UserBitShared.QueryType getValidationQueryType() throws Exception {
            return UserBitShared.QueryType.SQL;
        }
    }

    public class SchemaTestBuilder extends TestBuilderSelf {
        private List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema;

        SchemaTestBuilder(BufferAllocator allocator, Object query, UserBitShared.QueryType queryType, String baselineOptionSettingQueries, String testOptionSettingQueries, List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema) {
            super(allocator, query, queryType, false, false, (Map)null, baselineOptionSettingQueries, testOptionSettingQueries, false, -1, (Map)null, (Map)null);
            this.expectsEmptyResultSet();
            this.expectedSchema = expectedSchema;
        }

        public TestBuilderSelf baselineColumns(String... columns) {
            assert false : "The column information should be captured in expected scheme, not baselineColumns";

            return this;
        }

        public TestBuilderSelf baselineRecords(List<Map<String, Object>> materializedRecords) {
            assert false : "Since only schema will be compared in this test, no record is expected";

            return this;
        }

        public TestBuilderSelf baselineValues(Object... objects) {
            assert false : "Since only schema will be compared in this test, no record is expected";

            return this;
        }

        protected UserBitShared.QueryType getValidationQueryType() throws Exception {
            return null;
        }

        public List<Pair<SchemaPath, TypeProtos.MajorType>> getExpectedSchema() {
            return this.expectedSchema;
        }
    }

    public class CSVTestBuilder extends TestBuilderSelf {
        private String baselineFilePath;
        private TypeProtos.MajorType[] baselineTypes;

        CSVTestBuilder(String baselineFile, BufferAllocator allocator, Object query, UserBitShared.QueryType queryType, Boolean ordered, boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap, String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison, int expectedNumBatches, Map<String, DremioTestWrapper.BaselineValuesForTDigest> baselineTolerances, Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap) {
            super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison, expectedNumBatches, baselineTolerances, baselineValuesForItemsSketchMap);
            this.baselineFilePath = baselineFile;
        }

        public TestBuilderSelf.CSVTestBuilder baselineTypes(TypeProtos.MajorType... baselineTypes) {
            this.baselineTypes = baselineTypes;
            this.baselineTypeMap = null;
            return this;
        }

        public TestBuilderSelf.CSVTestBuilder baselineTypes(TypeProtos.MinorType... baselineTypes) {
            TypeProtos.MajorType[] majorTypes = new TypeProtos.MajorType[baselineTypes.length];
            int i = 0;
            TypeProtos.MinorType[] var4 = baselineTypes;
            int var5 = baselineTypes.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                TypeProtos.MinorType minorType = var4[var6];
                majorTypes[i] = Types.required(minorType);
                ++i;
            }

            this.baselineTypes = majorTypes;
            this.baselineTypeMap = null;
            return this;
        }

        protected TestBuilderSelf reset() {
            super.reset();
            this.baselineTypeMap = null;
            this.baselineTypes = null;
            this.baselineFilePath = null;
            return this;
        }

        boolean typeInfoSet() {
            return super.typeInfoSet() || this.baselineTypes != null;
        }

        String getValidationQuery() throws Exception {
            if (this.baselineColumns.length == 0) {
                throw new Exception("Baseline CSV files require passing column names, please call the baselineColumns() method on the test builder.");
            } else {
                if (this.baselineTypes != null) {
                    Assert.assertEquals("Must pass the same number of types as column names if types are provided.", (long)this.baselineTypes.length, (long)this.baselineColumns.length);
                }

                String[] aliasedExpectedColumns = new String[this.baselineColumns.length];

                for(int i = 0; i < this.baselineColumns.length; ++i) {
                    aliasedExpectedColumns[i] = "columns[" + i + "] ";
                    TypeProtos.MajorType majorType;
                    if (this.baselineTypes != null) {
                        majorType = this.baselineTypes[i];
                    } else {
                        if (this.baselineTypeMap == null) {
                            throw new Exception("Type information not set for interpreting csv baseline file.");
                        }

                        majorType = (TypeProtos.MajorType)this.baselineTypeMap.get(parsePath(this.baselineColumns[i]));
                    }

                    String precision = TestBuilderSelf.this.getDecimalPrecisionScaleInfo(majorType);
                    if (majorType.getMinorType() == TypeProtos.MinorType.VARCHAR || majorType.getMinorType() == TypeProtos.MinorType.VARBINARY) {
                        precision = "(65000)";
                    }

                    aliasedExpectedColumns[i] = "cast(" + aliasedExpectedColumns[i] + " as " + getNameOfMinorType(majorType.getMinorType()) + precision + " ) " + this.baselineColumns[i].replace('`', '"');
                }

                String query = "select " + Joiner.on(", ").join(aliasedExpectedColumns) + " from cp.\"" + this.baselineFilePath + "\"";
                return query;
            }
        }

        protected UserBitShared.QueryType getValidationQueryType() throws Exception {
            return UserBitShared.QueryType.SQL;
        }
    }
}

