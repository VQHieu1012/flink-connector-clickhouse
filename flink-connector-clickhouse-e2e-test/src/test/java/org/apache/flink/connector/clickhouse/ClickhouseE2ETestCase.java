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

import org.apache.flink.api.common.JobID;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.After;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.INSERT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** End-to-end test for Clickhouse. */
public class ClickhouseE2ETestCase extends FlinkContainerEnvironment {

    ClickhouseProxy proxy;

    private static final String CONTAINER_JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default";

    @Test
    public void testSink() throws Exception {
        createProxy();
        proxy.execute(
                "create table test_insert (id Int32, name String, float32_column Float32, date_column Date,datetime_column DateTime, array_column Array(Int32)) engine = Memory; ");
        List<String> sqlLines = new ArrayList<>();
        sqlLines.add(
                "create table test (id int, name varchar,float32_column FLOAT,\n"
                        + "    datetime_column TIMESTAMP(3),\n"
                        + "    array_column ARRAY<INT>) with ('connector' = 'clickhouse',\n"
                        + "  'url' = '"
                        + CONTAINER_JDBC_URL
                        + "',\n"
                        + "  'table-name' = 'test_insert',\n"
                        + "  'username'='test_username',\n"
                        + "  'password'='test_password',\n"
                        + "  'properties.compress' = 'false',\n"
                        + "  'properties.decompress' = 'false'\n"
                        + ");");
        sqlLines.add(
                "INSERT INTO test VALUES "
                        + "(1, 'Name1', CAST(1.1 AS FLOAT), TIMESTAMP '2022-01-01 00:00:00', ARRAY[1, 2, 3]),"
                        + "(2, 'Name2', CAST(2.2 AS FLOAT), TIMESTAMP '2022-01-02 01:00:00', ARRAY[4, 5, 6]),"
                        + "(3, 'Name3', CAST(3.3 AS FLOAT), TIMESTAMP '2022-01-03 02:00:00', ARRAY[7, 8, 9]),"
                        + "(4, 'Name4', CAST(4.4 AS FLOAT), TIMESTAMP '2022-01-04 03:00:00', ARRAY[10, 11, 12]),"
                        + "(5, 'Name5', CAST(5.5 AS FLOAT), TIMESTAMP '2022-01-05 04:00:00', ARRAY[13, 14, 15]);");

        submitClickHouseSQLJob(sqlLines);
        waitUntilJobRunning(Duration.of(1, ChronoUnit.MINUTES));
        List<String> expectedResult =
                Arrays.asList(
                        "1,Name1,1.1,2022-01-01 00:00:00,[1,2,3]",
                        "2,Name2,2.2,2022-01-02 01:00:00,[4,5,6]",
                        "3,Name3,3.3,2022-01-03 02:00:00,[7,8,9]",
                        "4,Name4,4.4,2022-01-04 03:00:00,[10,11,12]",
                        "5,Name5,5.5,2022-01-05 04:00:00,[13,14,15]");
        proxy.checkResultWithTimeout(
                expectedResult,
                "test_insert",
                Arrays.asList("id", "name", "float32_column", "datetime_column", "array_column"),
                600_000);
    }

    @Test
    public void testUpsertChangelog() throws Exception {
        createProxy();
        proxy.execute(
                "CREATE TABLE aggregate_sink (id Int32, total Int64) "
                        + "ENGINE = MergeTree ORDER BY id");

        List<String> sqlLines = new ArrayList<>();
        sqlLines.add(
                "CREATE TABLE aggregate_events (seq BIGINT) WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '3',\n"
                        + "  'fields.seq.kind' = 'sequence',\n"
                        + "  'fields.seq.start' = '1',\n"
                        + "  'fields.seq.end' = '3'\n"
                        + ");");
        sqlLines.add(
                clickHouseTableDdl(
                        "aggregate_sink_table",
                        "id INT, total BIGINT, PRIMARY KEY (id) NOT ENFORCED",
                        "aggregate_sink",
                        ",\n  'sink.batch-size' = '1',\n"
                                + "  'sink.ignore-delete' = 'false',\n"
                                + "  'properties.mutations_sync' = '2'"));
        sqlLines.add(
                "INSERT INTO aggregate_sink_table "
                        + "SELECT CAST(MOD(seq, 2) AS INT), SUM(seq) "
                        + "FROM aggregate_events GROUP BY MOD(seq, 2);");

        submitClickHouseSQLJob(sqlLines);
        waitUntilJobRunning(Duration.of(1, ChronoUnit.MINUTES));
        proxy.checkResultWithTimeout(
                Arrays.asList("0,2", "1,4"),
                "aggregate_sink",
                Arrays.asList("id", "total"),
                120_000);
    }

    @Test
    public void testDeleteMutationExecutor() throws Exception {
        createProxy();
        proxy.execute(
                "CREATE TABLE delete_sink (id Int32, name String) ENGINE = MergeTree ORDER BY id");
        Properties properties = new Properties();
        properties.setProperty("clickhouse_setting_mutations_sync", "2");
        ClickHouseDmlOptions options =
                new ClickHouseDmlOptions.Builder()
                        .withUrl(CLICKHOUSE_CONTAINER.getJdbcUrl())
                        .withUsername(CLICKHOUSE_CONTAINER.getUsername())
                        .withPassword(CLICKHOUSE_CONTAINER.getPassword())
                        .withDatabaseName("default")
                        .withTableName("delete_sink")
                        .withMaxRetries(0)
                        .withUpdateStrategy(UPDATE)
                        .withIgnoreDelete(false)
                        .build();
        ClickHouseExecutor executor =
                ClickHouseExecutor.createClickHouseExecutor(
                        "delete_sink",
                        "default",
                        null,
                        new String[] {"id", "name"},
                        new String[] {"id"},
                        new String[0],
                        new LogicalType[] {new IntType(), new VarCharType()},
                        options);
        ClickHouseConnectionProvider provider =
                new ClickHouseConnectionProvider(options, properties);
        try {
            executor.prepareStatement(provider);
            executor.addToBatch(
                    GenericRowData.ofKind(RowKind.INSERT, 1, StringData.fromString("delete-me")));
            executor.executeBatch();
            executor.addToBatch(
                    GenericRowData.ofKind(RowKind.DELETE, 1, StringData.fromString("delete-me")));
            executor.executeBatch();

            proxy.checkResultWithTimeout(
                    new ArrayList<>(), "delete_sink", Arrays.asList("id", "name"), 30_000);
        } finally {
            executor.closeStatement();
            provider.closeConnections();
        }
    }

    @Test
    public void testReplacingMergeTreeCdcUsesAppendOnlyTombstones() throws Exception {
        createProxy();
        proxy.execute(
                "CREATE TABLE replacing_cdc_sink ("
                        + "id Int32, name String, _version Int64, _is_deleted UInt8) "
                        + "ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY id");
        ClickHouseDmlOptions options =
                new ClickHouseDmlOptions.Builder()
                        .withUrl(CLICKHOUSE_CONTAINER.getJdbcUrl())
                        .withUsername(CLICKHOUSE_CONTAINER.getUsername())
                        .withPassword(CLICKHOUSE_CONTAINER.getPassword())
                        .withDatabaseName("default")
                        .withTableName("replacing_cdc_sink")
                        .withMaxRetries(0)
                        .withUpdateStrategy(INSERT)
                        .withIgnoreDelete(false)
                        .build();
        ClickHouseExecutor executor =
                ClickHouseExecutor.createClickHouseExecutor(
                        "replacing_cdc_sink",
                        "default",
                        null,
                        new String[] {"id", "name", "_version", "_is_deleted"},
                        new String[] {"id"},
                        new String[0],
                        new LogicalType[] {
                            new IntType(), new VarCharType(), new BigIntType(), new SmallIntType()
                        },
                        options);
        ClickHouseConnectionProvider provider = new ClickHouseConnectionProvider(options);
        try {
            executor.prepareStatement(provider);
            executor.addToBatch(cdcRow(RowKind.INSERT, 1, "old", 1L));
            executor.addToBatch(cdcRow(RowKind.UPDATE_AFTER, 1, "current", 2L));
            executor.addToBatch(cdcRow(RowKind.INSERT, 2, "deleted", 1L));
            executor.addToBatch(cdcRow(RowKind.DELETE, 2, "deleted", 2L));
            executor.executeBatch();

            proxy.checkResultWithTimeout(
                    Arrays.asList("1,current,2,0"),
                    "replacing_cdc_sink FINAL",
                    Arrays.asList("id", "name", "_version", "_is_deleted"),
                    30_000L);
        } finally {
            executor.closeStatement();
            provider.closeConnections();
        }
    }

    @Test
    public void testAmbiguousPostCommitFailureProducesDuplicate() throws Exception {
        createProxy();
        proxy.execute("CREATE TABLE ambiguous_sink (id Int64) ENGINE = Memory");
        ClickHouseDmlOptions options =
                new ClickHouseDmlOptions.Builder()
                        .withUrl(CLICKHOUSE_CONTAINER.getJdbcUrl())
                        .withUsername(CLICKHOUSE_CONTAINER.getUsername())
                        .withPassword(CLICKHOUSE_CONTAINER.getPassword())
                        .withDatabaseName("default")
                        .withTableName("ambiguous_sink")
                        .withMaxRetries(1)
                        .build();
        ClickHouseConnectionProvider provider = new ClickHouseConnectionProvider(options);
        ClickHouseExecutor executor =
                ClickHouseExecutor.createClickHouseExecutor(
                        "ambiguous_sink",
                        "default",
                        null,
                        new String[] {"id"},
                        new String[0],
                        new String[0],
                        new LogicalType[] {new BigIntType()},
                        options);
        AtomicInteger attempts = new AtomicInteger();
        SimpleCounter retries = new SimpleCounter();
        try {
            executor.attemptExecuteBatch(
                    () -> {
                        try (PreparedStatement statement =
                                provider.getOrCreateConnection()
                                        .prepareStatement(
                                                "INSERT INTO default.ambiguous_sink (id) VALUES (?)")) {
                            statement.setLong(1, 7L);
                            statement.addBatch();
                            statement.executeBatch();
                        }
                        if (attempts.getAndIncrement() == 0) {
                            throw new SQLRecoverableException(
                                    "simulated response loss after ClickHouse committed the batch");
                        }
                    },
                    1,
                    retries,
                    () -> provider.reconnect());

            assertEquals(2, attempts.get());
            assertEquals(1L, retries.getCount());
            proxy.checkResultWithTimeout(
                    Arrays.asList("7", "7"), "ambiguous_sink", Arrays.asList("id"), 30_000L);
        } finally {
            provider.closeConnections();
        }
    }

    @Test
    public void testCheckpointRecoveryAfterTaskManagerRestart() throws Exception {
        createProxy();
        proxy.execute(
                "CREATE TABLE recovery_sink (id Int64, payload String) "
                        + "ENGINE = MergeTree ORDER BY id");
        List<String> sqlLines = new ArrayList<>();
        sqlLines.add("SET 'execution.checkpointing.interval' = '1s';");
        sqlLines.add("SET 'execution.checkpointing.mode' = 'AT_LEAST_ONCE';");
        sqlLines.add(
                "CREATE TABLE recovery_source (id BIGINT, payload STRING) WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '20',\n"
                        + "  'fields.id.kind' = 'sequence',\n"
                        + "  'fields.id.start' = '1',\n"
                        + "  'fields.id.end' = '1000000',\n"
                        + "  'fields.payload.length' = '32'\n"
                        + ");");
        sqlLines.add(
                clickHouseTableDdl(
                        "recovery_sink_table",
                        "id BIGINT, payload STRING",
                        "recovery_sink",
                        ",\n  'sink.batch-size' = '5',\n" + "  'sink.flush-interval' = '200ms'"));
        sqlLines.add("INSERT INTO recovery_sink_table SELECT * FROM recovery_source;");

        submitClickHouseSQLJob(sqlLines);
        JobID jobId = waitUntilJobRunning(Duration.of(1, ChronoUnit.MINUTES));
        try {
            long rowsBeforeRestart = proxy.waitForRowCountAtLeast("recovery_sink", 20L, 60_000L);
            getRestClusterClient()
                    .triggerCheckpoint(jobId, CheckpointType.CONFIGURED)
                    .get(30L, TimeUnit.SECONDS);

            restartTaskManager();
            waitUntilJobRunning(Duration.of(1, ChronoUnit.MINUTES));

            proxy.waitForRowCountAtLeast("recovery_sink", rowsBeforeRestart + 20L, 90_000L);
        } finally {
            getRestClusterClient().cancel(jobId).get(30L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAsyncFlushFailureFailsJobWithoutAnotherSinkRecord() throws Exception {
        createProxy();
        proxy.execute("CREATE TABLE fail_fast_sink (id Int64) ENGINE = Memory");
        List<String> sqlLines = new ArrayList<>();
        sqlLines.add("SET 'restart-strategy.type' = 'none';");
        sqlLines.add(
                "CREATE TABLE fail_fast_source (id BIGINT) WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'fields.id.kind' = 'sequence',\n"
                        + "  'fields.id.start' = '1',\n"
                        + "  'fields.id.end' = '1000000'\n"
                        + ");");
        sqlLines.add(
                clickHouseTableDdl(
                        "fail_fast_sink_table",
                        "id BIGINT",
                        "fail_fast_sink",
                        ",\n  'sink.batch-size' = '1000',\n"
                                + "  'sink.flush-interval' = '200ms',\n"
                                + "  'sink.max-retries' = '0'"));
        sqlLines.add(
                "INSERT INTO fail_fast_sink_table "
                        + "SELECT id FROM fail_fast_source WHERE id = 1 OR id = 10;");

        submitClickHouseSQLJob(sqlLines);
        JobID jobId = waitUntilJobRunning(Duration.of(1, ChronoUnit.MINUTES));
        proxy.waitForRowCountAtLeast("fail_fast_sink", 1L, 30_000L);
        proxy.execute("DROP TABLE fail_fast_sink");

        String failureDetails = waitUntilJobFailed(jobId, Duration.of(1, ChronoUnit.MINUTES));
        assertTrue(failureDetails, failureDetails.contains("Asynchronous ClickHouse flush failed"));
    }

    private void createProxy() {
        proxy =
                new ClickhouseProxy(
                        CLICKHOUSE_CONTAINER.getJdbcUrl(),
                        CLICKHOUSE_CONTAINER.getUsername(),
                        CLICKHOUSE_CONTAINER.getPassword());
    }

    private GenericRowData cdcRow(RowKind kind, int id, String name, long version) {
        return GenericRowData.ofKind(kind, id, StringData.fromString(name), version, (short) 0);
    }

    private String clickHouseTableDdl(
            String flinkTable, String columns, String clickHouseTable, String extraOptions) {
        return "CREATE TABLE "
                + flinkTable
                + " ("
                + columns
                + ") WITH ('connector' = 'clickhouse',\n"
                + "  'url' = '"
                + CONTAINER_JDBC_URL
                + "',\n"
                + "  'table-name' = '"
                + clickHouseTable
                + "',\n"
                + "  'username' = 'test_username',\n"
                + "  'password' = 'test_password',\n"
                + "  'properties.compress' = 'false',\n"
                + "  'properties.decompress' = 'false'"
                + extraOptions
                + "\n);";
    }

    private void submitClickHouseSQLJob(List<String> sqlLines) throws Exception {
        submitSQLJob(sqlLines, SQL_CONNECTOR_CLICKHOUSE_JAR);
    }

    @After
    public void tearDown() {
        if (proxy != null) {
            proxy.close();
        }
    }
}
