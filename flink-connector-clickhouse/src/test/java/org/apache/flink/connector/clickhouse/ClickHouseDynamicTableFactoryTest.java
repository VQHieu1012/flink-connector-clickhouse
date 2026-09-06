/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;

import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_CDC_DELETED_COLUMN;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_CDC_VERSION_COLUMN;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_CONNECTION_TIMEOUT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_BUFFERED_BYTES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_SOCKET_TIMEOUT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_UPDATE_STRATEGY;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.INSERT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for eager sink option validation. */
public class ClickHouseDynamicTableFactoryTest {

    private final ClickHouseDynamicTableFactory factory = new ClickHouseDynamicTableFactory();

    @Test
    public void rejectsNonPositiveBatchSize() {
        Configuration config = new Configuration();
        config.set(SINK_BATCH_SIZE, 0);

        assertThrows(IllegalArgumentException.class, () -> factory.validateConfigOptions(config));
    }

    @Test
    public void rejectsNonPositiveBufferedBytes() {
        Configuration config = new Configuration();
        config.set(SINK_MAX_BUFFERED_BYTES, MemorySize.ZERO);

        assertThrows(IllegalArgumentException.class, () -> factory.validateConfigOptions(config));
    }

    @Test
    public void rejectsNonPositiveFlushInterval() {
        Configuration config = new Configuration();
        config.set(SINK_FLUSH_INTERVAL, Duration.ZERO);

        assertThrows(IllegalArgumentException.class, () -> factory.validateConfigOptions(config));
    }

    @Test
    public void rejectsNegativeRetryCount() {
        Configuration config = new Configuration();
        config.set(SINK_MAX_RETRIES, -1);

        assertThrows(IllegalArgumentException.class, () -> factory.validateConfigOptions(config));
    }

    @Test
    public void supportsConfigurableCdcColumnNames() {
        Configuration config = new Configuration();
        config.set(SINK_UPDATE_STRATEGY, INSERT);
        config.set(SINK_CDC_VERSION_COLUMN, "source_lsn");
        config.set(SINK_CDC_DELETED_COLUMN, "tombstone");

        factory.validateConfigOptions(config);
        ClickHouseDmlOptions options = factory.getDmlOptions(config);

        assertEquals("source_lsn", options.getCdcVersionColumn());
        assertEquals("tombstone", options.getCdcDeletedColumn());
    }

    @Test
    public void rejectsInvalidCdcColumnNames() {
        Configuration duplicateColumns = new Configuration();
        duplicateColumns.set(SINK_UPDATE_STRATEGY, INSERT);
        duplicateColumns.set(SINK_CDC_VERSION_COLUMN, "cdc_metadata");
        duplicateColumns.set(SINK_CDC_DELETED_COLUMN, "cdc_metadata");
        assertThrows(
                IllegalArgumentException.class,
                () -> factory.validateConfigOptions(duplicateColumns));

        Configuration blankVersionColumn = new Configuration();
        blankVersionColumn.set(SINK_UPDATE_STRATEGY, INSERT);
        blankVersionColumn.set(SINK_CDC_VERSION_COLUMN, "  ");
        assertThrows(
                IllegalArgumentException.class,
                () -> factory.validateConfigOptions(blankVersionColumn));
    }

    @Test
    public void rejectsNonPositiveParallelism() {
        Configuration config = new Configuration();
        config.set(SINK_PARALLELISM, 0);

        assertThrows(IllegalArgumentException.class, () -> factory.validateConfigOptions(config));
    }

    @Test
    public void rejectsNonPositiveConnectionTimeouts() {
        Configuration invalidConnectionTimeout = new Configuration();
        invalidConnectionTimeout.set(SINK_CONNECTION_TIMEOUT, Duration.ZERO);

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.validateConfigOptions(invalidConnectionTimeout));

        Configuration invalidSocketTimeout = new Configuration();
        invalidSocketTimeout.set(SINK_SOCKET_TIMEOUT, Duration.ofMillis(-1));
        assertThrows(
                IllegalArgumentException.class,
                () -> factory.validateConfigOptions(invalidSocketTimeout));
    }

    @Test
    public void appliesSinkTimeoutsWithoutOverwritingDriverProperties() {
        Configuration config = new Configuration();
        config.set(SINK_CONNECTION_TIMEOUT, Duration.ofSeconds(2));
        config.set(SINK_SOCKET_TIMEOUT, Duration.ofSeconds(30));
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.socket_timeout", "1234");
        tableOptions.put("properties.mutations_sync", "2");

        Properties properties = factory.getSinkConnectionProperties(config, tableOptions);

        assertEquals("2000", properties.getProperty("connection_timeout"));
        assertEquals("1234", properties.getProperty("socket_timeout"));
        assertEquals("2", properties.getProperty("clickhouse_setting_mutations_sync"));
    }

    @Test
    public void rejectsStaticInsertDeduplicationToken() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.insert_deduplication_token", "static-token");

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.getSinkConnectionProperties(new Configuration(), tableOptions));
    }

    @Test
    public void rejectsLegacyConnectTimeoutProperty() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.connect_timeout", "1000");

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.getSinkConnectionProperties(new Configuration(), tableOptions));
    }

    @Test
    public void rejectsUnsafeFireAndForgetAsyncInsert() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.async_insert", "1");
        tableOptions.put("properties.wait_for_async_insert", "0");

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.getSinkConnectionProperties(new Configuration(), tableOptions));
    }
}
