package com.dremio.exec.store.jdbc.conf;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.protostuff.Tag;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@SourceType(value = "CRATEDB", label = "CRATEDB", uiConfig = "crate-layout.json")
public class CrateConf extends AbstractArpConf<CrateConf> {
    private static final String ARP_FILENAME = "arp/implementation/crate-arp.yaml";
    private static final Logger logger = Logger.getLogger(CrateConf.class);
    private static final ArpDialect ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, CratedbDialect::new);
    private static final String DRIVER = "io.crate.client.jdbc.CrateDriver";
    static class CratedbSchemaFetcher extends ArpDialect.ArpSchemaFetcher {
        private final String query;
        @VisibleForTesting
        public String getQuery() {
            return this.query;
        }
        public CratedbSchemaFetcher(String query, JdbcPluginConfig config) {
            super(query, config);
            this.query = query;
        }

        @Override
        public JdbcFetcherProto.GetTableMetadataResponse getTableMetadata(JdbcFetcherProto.GetTableMetadataRequest request) {
            JdbcFetcherProto.GetTableMetadataRequest newRequest  = JdbcFetcherProto.GetTableMetadataRequest.newBuilder().setTable(request.getTable()).setSchema(request.getSchema()).build();
            return super.getTableMetadata(newRequest);
        }

        @Override
        public CloseableIterator<JdbcFetcherProto.CanonicalizeTablePathResponse> listTableNames(JdbcFetcherProto.ListTableNamesRequest request) {
            try {
                Connection connection = this.dataSource.getConnection();
                ArpJdbcTableNamesIterator arpJdbcTableNamesIterator = new ArpJdbcTableNamesIterator(this.config.getSourceName(), connection, this.query);
                return arpJdbcTableNamesIterator;
            } catch (SQLException var3) {
                return EmptyCloseableIterator.getInstance();
            }
        }

        @Override
        public JdbcFetcherProto.CanonicalizeTablePathResponse canonicalizeTablePath(JdbcFetcherProto.CanonicalizeTablePathRequest request) {
            try {
                Connection connection = this.dataSource.getConnection();
                Throwable throwable = null;
                JdbcFetcherProto.CanonicalizeTablePathResponse canonicalizeTablePathResponse;
                try {
                    if ((!this.usePrepareForColumnMetadata() || !this.config.shouldSkipSchemaDiscovery()) && !this.usePrepareForGetTables()) {
                        canonicalizeTablePathResponse = this.getDatasetHandleViaGetTables(request, connection);
                        return canonicalizeTablePathResponse;
                    }
                    canonicalizeTablePathResponse = this.getTableHandleViaPrepare(request, connection);
                } catch (Throwable var9) {
                    throwable = var9;
                    throw var9;
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
                }

                return canonicalizeTablePathResponse;
            } catch (SQLException sqlException) {
                logger.warn(String.format("Failed to fetch schema for %s.", request));
                return JdbcFetcherProto.CanonicalizeTablePathResponse.getDefaultInstance();
            }
        }
        private JdbcFetcherProto.CanonicalizeTablePathResponse getDatasetHandleViaGetTables(JdbcFetcherProto.CanonicalizeTablePathRequest request, Connection connection) throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tablesResult = metaData.getTables("", "doc", null, (String[])null);
            Throwable throwable = null;
            try {
                while(tablesResult.next()) {
                        com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder responseBuilder = JdbcFetcherProto.CanonicalizeTablePathResponse.newBuilder();
                        logger.info(String.format("table name  %s", tablesResult.getString(3)));
                        responseBuilder.setTable(tablesResult.getString(3));
                        JdbcFetcherProto.CanonicalizeTablePathResponse var10 = responseBuilder.build();
                        return var10;
                }
            } catch (Throwable throwable1) {
                throwable = throwable1;
                throw throwable1;
            } finally {
                if (tablesResult != null) {
                    tablesResult.close();
                }
            }
            return JdbcFetcherProto.CanonicalizeTablePathResponse.getDefaultInstance();
        }
        private List<String> getEntities(JdbcFetcherProto.CanonicalizeTablePathRequest request, String pluginName) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            if (!Strings.isNullOrEmpty(request.getTable())) {
                builder.add(request.getTable());
            }
            return builder.build();
        }
        private JdbcFetcherProto.CanonicalizeTablePathResponse getTableHandleViaPrepare(JdbcFetcherProto.CanonicalizeTablePathRequest request, Connection connection) throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();
            List<String> trimmedList = this.getEntities(request, (String)null);
            logger.info(String.format("trimmedList %s", trimmedList.toString()));
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + this.getQuotedPath(trimmedList));
            Throwable throwable = null;

            JdbcFetcherProto.CanonicalizeTablePathResponse canonicalizeTablePathResponse;
            try {
                ResultSetMetaData preparedMetadata = statement.getMetaData();
                if (preparedMetadata.getColumnCount() <= 0) {
                    logger.debug("Table has no columns, query is in invalid");
                    JdbcFetcherProto.CanonicalizeTablePathResponse canonicalizeTablePathResponse1 = JdbcFetcherProto.CanonicalizeTablePathResponse.getDefaultInstance();
                    return canonicalizeTablePathResponse1;
                }
                com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder responseBuilder = JdbcFetcherProto.CanonicalizeTablePathResponse.newBuilder();
                responseBuilder.setTable(request.getTable());
                canonicalizeTablePathResponse = responseBuilder.build();
            } catch (Throwable throwable1) {
                throwable = throwable1;
                throw throwable1;
            } finally {
                if (statement != null) {
                    statement.close();
                }

            }
            return canonicalizeTablePathResponse;
        }
        @Override
        protected boolean usePrepareForColumnMetadata() {
            return true;
        }

        protected boolean usePrepareForGetTables() {
            return true;
        }
        private static class EmptyCloseableIterator extends AbstractIterator<JdbcFetcherProto.CanonicalizeTablePathResponse> implements CloseableIterator<JdbcFetcherProto.CanonicalizeTablePathResponse> {
            private static EmptyCloseableIterator singleInstance;

            private EmptyCloseableIterator() {
            }

            static EmptyCloseableIterator getInstance() {
                if (singleInstance == null) {
                    singleInstance = new EmptyCloseableIterator();
                }

                return singleInstance;
            }

            protected JdbcFetcherProto.CanonicalizeTablePathResponse computeNext() {
                return (JdbcFetcherProto.CanonicalizeTablePathResponse)this.endOfData();
            }

            public void close() {
            }
        }
        protected static class ArpJdbcTableNamesIterator extends AbstractIterator<JdbcFetcherProto.CanonicalizeTablePathResponse> implements CloseableIterator<JdbcFetcherProto.CanonicalizeTablePathResponse> {
            private final String storagePluginName;
            private final Connection connection;
            private Statement statement;
            private ResultSet tablesResult;

            protected ArpJdbcTableNamesIterator(String storagePluginName, Connection connection, String query) throws SQLException {
                this.storagePluginName = storagePluginName;
                this.connection = connection;
                try {
                    this.statement = connection.createStatement();
                    logger.info(String.format("ArpJdbcTableNamesIterator query:--------%s",query));

                    this.tablesResult = this.statement.executeQuery(query);
                } catch (SQLException var5) {
                }

            }

            public JdbcFetcherProto.CanonicalizeTablePathResponse computeNext() {
                try {
                    if (this.tablesResult != null && this.tablesResult.next()) {
                        com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder response = JdbcFetcherProto.CanonicalizeTablePathResponse.newBuilder();
                        response.setTable(this.tablesResult.getString(3));
                        return response.build();
                    } else {
                        return (JdbcFetcherProto.CanonicalizeTablePathResponse)this.endOfData();
                    }
                } catch (SQLException var4) {
                    return (JdbcFetcherProto.CanonicalizeTablePathResponse)this.endOfData();
                }
            }

            public void close() throws Exception {
                try {
                    AutoCloseables.close(new AutoCloseable[]{this.tablesResult, this.statement, this.connection});
                } catch (Exception var2) {
                }

            }
        }
    }

    static class CratedbDialect extends ArpDialect {

        public CratedbDialect(ArpYaml yaml) {
            super(yaml);
        }

        @Override
        public JdbcSchemaFetcherImpl newSchemaFetcher(JdbcPluginConfig config) {
            String query = "SELECT '' CAT,  '' SCH, TABLE_NAME NME from information_schema.tables where 1=1  and table_schema='doc'";

            return new CratedbSchemaFetcher(query,config);
        }
        @Override
        public ContainerSupport supportsCatalogs() {
            return ContainerSupport.UNSUPPORTED;
        }

        @Override
        public ContainerSupport supportsSchemas() {
            return ContainerSupport.UNSUPPORTED;
        }

        public boolean supportsNestedAggregations() {
            return false;
        }
    }
    @Tag(1)
    @DisplayMetadata(label = "username")
    @NotMetadataImpacting
    public String username = "crate";

    @Tag(2)
    @DisplayMetadata(label = "host")
    public String host;

    @Tag(3)
    @Secret
    @DisplayMetadata(label = "password")
    @NotMetadataImpacting
    public String password = "";

    @Tag(4)
    @DisplayMetadata(label = "port")
    @NotMetadataImpacting
    public int port = 5432;

    @Tag(5)
    @DisplayMetadata(label = "Record fetch size")
    @NotMetadataImpacting
    public int fetchSize = 200;


    @VisibleForTesting
    public String toJdbcConnectionString() {
        final String username = checkNotNull(this.username, "Missing username.");
        // format crate://localhost:5433/
        final String format = String.format("crate://%s:%d/", this.host, this.port);
        logger.info(format);

        return format;
    }
    @Override
    @VisibleForTesting
    public JdbcPluginConfig buildPluginConfig(
            JdbcPluginConfig.Builder configBuilder,
            CredentialsService credentialsService,
            OptionManager optionManager
    ) {

        return configBuilder.withDialect(getDialect())
                .withFetchSize(fetchSize)
                .clearHiddenSchemas()
                .addHiddenSchema("sys","pg_catalog","information_schema")
                .withDatasourceFactory(this::newDataSource)
                .build();
    }

    private CloseableDataSource newDataSource() throws SQLException {
        Properties properties = new Properties();
        logger.info("demo app ");
        CloseableDataSource dataSource = DataSources.newGenericConnectionPoolDataSource(DRIVER,
                toJdbcConnectionString(), this.username, this.password, properties, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
        return  dataSource;
    }

    @Override
    public ArpDialect getDialect() {
        return ARP_DIALECT;
    }
}
