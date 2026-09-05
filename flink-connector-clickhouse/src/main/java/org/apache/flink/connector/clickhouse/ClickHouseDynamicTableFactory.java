/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkShardingStrategy;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.DriverProperties;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.IDENTIFIER;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfig.PROPERTIES_PREFIX;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.CATALOG_IGNORE_PRIMARY_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.DATABASE_NAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.PASSWORD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_COLUMN;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_LOWER_BOUND;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_NUM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SCAN_PARTITION_UPPER_BOUND;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_CONNECTION_TIMEOUT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_BUFFERED_BYTES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_KEY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARTITION_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_SHARDING_USE_TABLE_DEF;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_SOCKET_TIMEOUT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_UPDATE_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.TABLE_NAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.URL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USERNAME;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.USE_LOCAL;
import static org.apache.flink.connector.clickhouse.util.ClickHouseUtil.getClickHouseProperties;

/** A {@link DynamicTableSinkFactory} for discovering {@link ClickHouseDynamicTableSink}. */
public class ClickHouseDynamicTableFactory
        implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    static final String JDBC_CONNECTION_TIMEOUT_PROPERTY = "connection_timeout";
    static final String JDBC_SOCKET_TIMEOUT_PROPERTY = "socket_timeout";
    private static final String CLICKHOUSE_SETTING_PREFIX = "clickhouse_setting_";
    private static final String HTTP_HEADER_PREFIX = "http_header_";
    private static final Set<String> JDBC_PROPERTY_KEYS = createJdbcPropertyKeys();

    public ClickHouseDynamicTableFactory() {}

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validateExcept(PROPERTIES_PREFIX);
        validateConfigOptions(config);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        String[] primaryKeys =
                catalogTable
                        .getResolvedSchema()
                        .getPrimaryKey()
                        .map(UniqueConstraint::getColumns)
                        .map(keys -> keys.toArray(new String[0]))
                        .orElse(new String[0]);
        Properties clickHouseProperties =
                getSinkConnectionProperties(config, context.getCatalogTable().getOptions());
        return new ClickHouseDynamicTableSink(
                getDmlOptions(config),
                clickHouseProperties,
                primaryKeys,
                catalogTable.getPartitionKeys().toArray(new String[0]),
                context.getPhysicalRowDataType());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validateExcept(PROPERTIES_PREFIX);
        validateConfigOptions(config);

        Properties clickHouseProperties =
                getClickHouseProperties(context.getCatalogTable().getOptions());
        return new ClickHouseDynamicTableSource(
                getReadOptions(config),
                helper.getOptions().get(LookupOptions.MAX_RETRIES),
                getLookupCache(config),
                clickHouseProperties,
                context.getPhysicalRowDataType());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(DATABASE_NAME);
        optionalOptions.add(USE_LOCAL);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_MAX_BUFFERED_BYTES);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_CONNECTION_TIMEOUT);
        optionalOptions.add(SINK_SOCKET_TIMEOUT);
        optionalOptions.add(SINK_UPDATE_STRATEGY);
        optionalOptions.add(SINK_PARTITION_STRATEGY);
        optionalOptions.add(SINK_PARTITION_KEY);
        optionalOptions.add(SINK_SHARDING_USE_TABLE_DEF);
        optionalOptions.add(SINK_IGNORE_DELETE);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(CATALOG_IGNORE_PRIMARY_KEY);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(LookupOptions.CACHE_TYPE);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
        optionalOptions.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        optionalOptions.add(LookupOptions.MAX_RETRIES);
        return optionalOptions;
    }

    void validateConfigOptions(ReadableConfig config) {
        if (config.get(SINK_BATCH_SIZE) <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' must be greater than zero.", SINK_BATCH_SIZE.key()));
        }
        if (config.get(SINK_MAX_BUFFERED_BYTES).getBytes() <= 0L) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' must be greater than zero.",
                            SINK_MAX_BUFFERED_BYTES.key()));
        }
        if (config.get(SINK_FLUSH_INTERVAL).isZero()
                || config.get(SINK_FLUSH_INTERVAL).isNegative()) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' must be greater than zero.",
                            SINK_FLUSH_INTERVAL.key()));
        }
        if (config.get(SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' must not be negative.", SINK_MAX_RETRIES.key()));
        }
        validatePositiveDuration(config, SINK_CONNECTION_TIMEOUT);
        validatePositiveDuration(config, SINK_SOCKET_TIMEOUT);
        config.getOptional(SINK_PARALLELISM)
                .filter(parallelism -> parallelism <= 0)
                .ifPresent(
                        parallelism -> {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "The value of '%s' must be greater than zero.",
                                            SINK_PARALLELISM.key()));
                        });

        // check sharding strategy and sharding key.
        SinkShardingStrategy shardingStrategy = config.get(SINK_PARTITION_STRATEGY);
        if (!config.get(SINK_SHARDING_USE_TABLE_DEF)
                && shardingStrategy.shardingKeyNeeded
                && !config.getOptional(SINK_PARTITION_KEY).isPresent()) {
            throw new IllegalArgumentException(
                    "A sharding key must be provided for sharding strategy: "
                            + shardingStrategy.value);
        }

        // check username and password.
        if (config.getOptional(USERNAME).isPresent() ^ config.getOptional(PASSWORD).isPresent()) {
            throw new IllegalArgumentException(
                    "Either all or none of username and password should be provided");
        }

        // check cache type.
        LookupOptions.LookupCacheType lookupCacheType = config.get(LookupOptions.CACHE_TYPE);
        if (!lookupCacheType.equals(LookupOptions.LookupCacheType.NONE)
                && !lookupCacheType.equals(LookupOptions.LookupCacheType.PARTIAL)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option should be 'NONE' or 'PARTIAL'(not support 'FULL' yet), but is %s.",
                            LookupOptions.CACHE_TYPE.key(), config.get(LookupOptions.CACHE_TYPE)));
        }

        // check scan partition config.
        boolean partitionColumnPresent = config.getOptional(SCAN_PARTITION_COLUMN).isPresent();
        if (partitionColumnPresent != config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent()
                || partitionColumnPresent
                        != config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Either all or none of the scan partition options should be provided:  %s, %s, %s.",
                            SCAN_PARTITION_COLUMN.key(),
                            SCAN_PARTITION_LOWER_BOUND.key(),
                            SCAN_PARTITION_UPPER_BOUND.key()));
        }

        // check max retries.
        if (config.get(LookupOptions.MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LookupOptions.MAX_RETRIES.key(),
                            config.get(LookupOptions.MAX_RETRIES)));
        }
    }

    private void validatePositiveDuration(
            ReadableConfig config, ConfigOption<java.time.Duration> option) {
        if (config.get(option).isZero() || config.get(option).isNegative()) {
            throw new IllegalArgumentException(
                    String.format("The value of '%s' must be greater than zero.", option.key()));
        }
    }

    Properties getSinkConnectionProperties(
            ReadableConfig config, java.util.Map<String, String> tableOptions) {
        Properties properties = getClickHouseProperties(tableOptions);
        validateSinkSemanticProperties(properties);
        properties = normalizeServerSettings(properties);
        properties.putIfAbsent(
                JDBC_CONNECTION_TIMEOUT_PROPERTY,
                Long.toString(config.get(SINK_CONNECTION_TIMEOUT).toMillis()));
        properties.putIfAbsent(
                JDBC_SOCKET_TIMEOUT_PROPERTY,
                Long.toString(config.get(SINK_SOCKET_TIMEOUT).toMillis()));
        return properties;
    }

    private Properties normalizeServerSettings(Properties properties) {
        Properties normalized = new Properties();
        for (String key : properties.stringPropertyNames()) {
            String normalizedKey = isJdbcProperty(key) ? key : DriverProperties.serverSetting(key);
            normalized.setProperty(normalizedKey, properties.getProperty(key));
        }
        return normalized;
    }

    private boolean isJdbcProperty(String key) {
        return JDBC_PROPERTY_KEYS.contains(key)
                || key.startsWith(CLICKHOUSE_SETTING_PREFIX)
                || key.startsWith(HTTP_HEADER_PREFIX);
    }

    private static Set<String> createJdbcPropertyKeys() {
        Set<String> keys = new HashSet<>();
        for (ClientConfigProperties property : ClientConfigProperties.values()) {
            keys.add(property.getKey());
        }
        for (DriverProperties property : DriverProperties.values()) {
            keys.add(property.getKey());
        }
        // Backward-compatible aliases handled by the ClickHouse JDBC driver.
        keys.add("compress");
        keys.add("decompress");
        return keys;
    }

    private void validateSinkSemanticProperties(Properties properties) {
        if (properties.containsKey("connect_timeout")) {
            throw new IllegalArgumentException(
                    "JDBC 0.9.x does not support 'properties.connect_timeout'; use "
                            + "'properties.connection_timeout' instead.");
        }
        if (properties.containsKey("insert_deduplication_token")
                || properties.containsKey(
                        DriverProperties.serverSetting("insert_deduplication_token"))) {
            throw new IllegalArgumentException(
                    "Static 'properties.insert_deduplication_token' is not supported because the "
                            + "same token would be reused for independent sink batches and could "
                            + "silently discard valid data.");
        }
        if (isEnabled(getServerSetting(properties, "async_insert"))
                && isDisabled(getServerSetting(properties, "wait_for_async_insert"))) {
            throw new IllegalArgumentException(
                    "'properties.wait_for_async_insert' must be enabled when "
                            + "'properties.async_insert' is enabled so ClickHouse acknowledges "
                            + "only flushed inserts.");
        }
    }

    private String getServerSetting(Properties properties, String key) {
        String value = properties.getProperty(key);
        return value != null ? value : properties.getProperty(DriverProperties.serverSetting(key));
    }

    private boolean isEnabled(String value) {
        return "1".equals(value) || Boolean.parseBoolean(value);
    }

    private boolean isDisabled(String value) {
        return "0".equals(value) || "false".equalsIgnoreCase(value);
    }

    private ClickHouseDmlOptions getDmlOptions(ReadableConfig config) {
        return new ClickHouseDmlOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withBatchSize(config.get(SINK_BATCH_SIZE))
                .withMaxBufferedBytes(config.get(SINK_MAX_BUFFERED_BYTES).getBytes())
                .withFlushInterval(config.get(SINK_FLUSH_INTERVAL))
                .withMaxRetries(config.get(SINK_MAX_RETRIES))
                .withUseLocal(config.get(USE_LOCAL))
                .withUpdateStrategy(config.get(SINK_UPDATE_STRATEGY))
                .withShardingStrategy(config.get(SINK_PARTITION_STRATEGY))
                .withShardingKey(config.get(SINK_PARTITION_KEY))
                .withUseTableDef(config.get(SINK_SHARDING_USE_TABLE_DEF))
                .withIgnoreDelete(config.get(SINK_IGNORE_DELETE))
                .withParallelism(config.get(SINK_PARALLELISM))
                .build();
    }

    private ClickHouseReadOptions getReadOptions(ReadableConfig config) {
        return new ClickHouseReadOptions.Builder()
                .withUrl(config.get(URL))
                .withUsername(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .withDatabaseName(config.get(DATABASE_NAME))
                .withTableName(config.get(TABLE_NAME))
                .withUseLocal(config.get(USE_LOCAL))
                .withPartitionColumn(config.get(SCAN_PARTITION_COLUMN))
                .withPartitionNum(config.get(SCAN_PARTITION_NUM))
                .withPartitionLowerBound(config.get(SCAN_PARTITION_LOWER_BOUND))
                .withPartitionUpperBound(config.get(SCAN_PARTITION_UPPER_BOUND))
                .build();
    }

    @Nullable
    private LookupCache getLookupCache(ReadableConfig tableOptions) {
        LookupCache cache = null;
        if (tableOptions
                .get(LookupOptions.CACHE_TYPE)
                .equals(LookupOptions.LookupCacheType.PARTIAL)) {
            cache = DefaultLookupCache.fromConfig(tableOptions);
        }
        return cache;
    }
}
