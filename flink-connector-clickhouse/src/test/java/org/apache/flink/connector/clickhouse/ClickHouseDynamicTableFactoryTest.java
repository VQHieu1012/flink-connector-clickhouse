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

import com.clickhouse.client.config.ClickHouseClientOption;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_CONNECTION_TIMEOUT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_BUFFERED_BYTES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SINK_SOCKET_TIMEOUT;
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

        Properties properties = factory.getSinkConnectionProperties(config, tableOptions);

        assertEquals(
                "2000", properties.getProperty(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey()));
        assertEquals(
                "1234", properties.getProperty(ClickHouseClientOption.SOCKET_TIMEOUT.getKey()));
    }
}
