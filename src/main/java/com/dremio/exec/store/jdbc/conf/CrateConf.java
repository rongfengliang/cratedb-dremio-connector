package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcherImpl;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@SourceType(value = "CRATEDB", label = "CRATEDB", uiConfig = "crate-layout.json")
public class CrateConf extends AbstractArpConf<CrateConf> {
    private static final String ARP_FILENAME = "arp/implementation/crate-arp.yaml";
    private static final ArpDialect ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, CratedbDialect::new);
    private static final String DRIVER = "io.crate.client.jdbc.CrateDriver";
    static class CratedbSchemaFetcher extends JdbcSchemaFetcherImpl {

        public CratedbSchemaFetcher(JdbcPluginConfig config) {
            super(config);
        }
        protected boolean usePrepareForColumnMetadata() {
            return true;
        }
        protected boolean usePrepareForGetTables() {
            return true;
        }
    }
    static class CratedbDialect extends ArpDialect {

        public CratedbDialect(ArpYaml yaml) {
            super(yaml);
        }

        @Override
        public JdbcSchemaFetcherImpl newSchemaFetcher(JdbcPluginConfig config) {
            return new CratedbSchemaFetcher(config);
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
//                .withSkipSchemaDiscovery(true)
                .clearHiddenSchemas()
                .addHiddenSchema("sys")
                .withDatasourceFactory(this::newDataSource)
                .build();
    }

    private CloseableDataSource newDataSource() throws SQLException {
        Properties properties = new Properties();
        CloseableDataSource dataSource = DataSources.newGenericConnectionPoolDataSource(DRIVER,
                toJdbcConnectionString(), this.username, this.password, properties, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
        return  dataSource;
    }

    @Override
    public ArpDialect getDialect() {
        return ARP_DIALECT;
    }
}
