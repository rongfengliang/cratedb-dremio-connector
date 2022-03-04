package com.dremio.exec.store.jdbc.conf;

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
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.protostuff.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@SourceType(value = "CRATEDB", label = "CRATEDB", uiConfig = "crate-layout.json")
public class CrateConf extends AbstractArpConf<CrateConf> {
    private static final String ARP_FILENAME = "arp/implementation/crate-arp.yaml";
    private static final ArpDialect ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, CratedbDialect::new);
    private static final String DRIVER = "io.crate.client.jdbc.CrateDriver";

    static class CratedbSchemaFetcherV2 extends ArpDialect.ArpSchemaFetcher {
        private static final Logger logger = LoggerFactory.getLogger(CratedbSchemaFetcherV2.class);
        private final JdbcPluginConfig config;

        public CratedbSchemaFetcherV2(String query, JdbcPluginConfig config) {
            super(query, config);
            this.config = config;
            logger.info("query schema:{}", query);
        }

        @Override
        public JdbcFetcherProto.GetTableMetadataResponse getTableMetadata(JdbcFetcherProto.GetTableMetadataRequest request) {
            return super.getTableMetadata(request);
        }

        @Override
        protected JdbcFetcherProto.CanonicalizeTablePathResponse getDatasetHandleViaGetTables(JdbcFetcherProto.CanonicalizeTablePathRequest request, Connection connection) throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();
            FilterDescriptor filter = new FilterDescriptor(request, supportsCatalogsWithoutSchemas(this.config.getDialect(), metaData));
            ResultSet tablesResult = metaData.getTables(filter.catalogName, filter.schemaName, filter.tableName, (String[]) null);
            Throwable throwable = null;

            JdbcFetcherProto.CanonicalizeTablePathResponse canonicalizeTablePathResponse;
            try {
                String currSchema;
                do {
                    if (!tablesResult.next()) {
                        return JdbcFetcherProto.CanonicalizeTablePathResponse.getDefaultInstance();
                    }
                    currSchema = tablesResult.getString(2);
                } while (!Strings.isNullOrEmpty(currSchema) && this.config.getHiddenSchemas().contains(currSchema));
                com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder responseBuilder = JdbcFetcherProto.CanonicalizeTablePathResponse.newBuilder();
                // cratedb not support catalog,but default implement fetch it so omit it
                if (!Strings.isNullOrEmpty(currSchema)) {
                    responseBuilder.setSchema(currSchema);
                }
                responseBuilder.setTable(tablesResult.getString(3));
                canonicalizeTablePathResponse = responseBuilder.build();
            } catch (Throwable ex) {
                throwable = ex;
                throw ex;
            } finally {
                if (tablesResult != null) {
                    try {
                        closeResource(throwable, tablesResult);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
            return canonicalizeTablePathResponse;
        }

        private static void closeResource(Throwable throwable, AutoCloseable autoCloseable) throws Exception {
            if (throwable != null) {
                try {
                    autoCloseable.close();
                } catch (Throwable throwable1) {
                    throwable.addSuppressed(throwable1);
                }
            } else {
                autoCloseable.close();
            }

        }

        protected static class FilterDescriptor {
            private final String catalogName;
            private final String schemaName;
            private final String tableName;

            public FilterDescriptor(JdbcFetcherProto.CanonicalizeTablePathRequest request, boolean hasCatalogsWithoutSchemas) {
                this.tableName = request.getTable();
                if (!Strings.isNullOrEmpty(request.getSchema())) {
                    this.schemaName = request.getSchema();
                    this.catalogName = request.getCatalogOrSchema();
                } else {
                    this.catalogName = hasCatalogsWithoutSchemas ? request.getCatalogOrSchema() : "";
                    this.schemaName = hasCatalogsWithoutSchemas ? "" : request.getCatalogOrSchema();
                }

            }
        }
    }

    static class CratedbDialect extends ArpDialect {
        public CratedbDialect(ArpYaml yaml) {
            super(yaml);
        }

        @Override
        public ArpSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
            String query = String.format("SELECT NULL, SCH, NME from ( select table_catalog CAT, table_schema SCH, table_name NME from information_schema.\"tables\" union all select table_catalog CAT, table_schema SCH,table_name NME from information_schema.views ) t where cat not in ('information_schema','pg_catalog','sys', '%s')", new Object[]{Joiner.on("','").join(config.getHiddenSchemas())});
            return new CratedbSchemaFetcherV2(query, config);
        }

        @Override
        public ContainerSupport supportsCatalogs() {
            return ContainerSupport.UNSUPPORTED;
        }

        @Override
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


    @Tag(6)
    @DisplayMetadata(
            label = "Maximum idle connections"
    )
    @NotMetadataImpacting
    public int maxIdleConns = 8;

    @Tag(7)
    @DisplayMetadata(
            label = "Connection idle time (s)"
    )
    @NotMetadataImpacting
    public int idleTimeSec = 60;

    @VisibleForTesting
    public String toJdbcConnectionString() {
        checkNotNull(this.username, "Missing username.");
        // format crate://localhost:5433/
        final String format = String.format("crate://%s:%d/", this.host, this.port);
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
                .addHiddenSchema("sys")
                .withDatasourceFactory(this::newDataSource)
                .build();
    }

    private CloseableDataSource newDataSource() {
        Properties properties = new Properties();
        CloseableDataSource dataSource = DataSources.newGenericConnectionPoolDataSource(DRIVER,
                toJdbcConnectionString(), this.username, this.password, properties, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE, this.maxIdleConns, this.idleTimeSec);
        return dataSource;
    }

    @Override
    public ArpDialect getDialect() {
        return ARP_DIALECT;
    }
}
