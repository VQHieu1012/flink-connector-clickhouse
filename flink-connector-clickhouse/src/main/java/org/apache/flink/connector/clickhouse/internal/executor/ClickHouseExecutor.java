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

package org.apache.flink.connector.clickhouse.internal.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.clickhouse.internal.ClickHouseStatementFactory;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseStatementWrapper;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Executor interface for submitting data to ClickHouse. */
public interface ClickHouseExecutor extends Serializable {

    Logger LOG = LoggerFactory.getLogger(ClickHouseExecutor.class);

    void prepareStatement(Connection connection) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void setRetryCounter(Counter retryCounter);

    void addToBatch(RowData rowData) throws SQLException;

    void executeBatch() throws SQLException;

    long getBufferedBytes();

    void closeStatement();

    default void attemptExecuteBatch(
            ClickHouseStatementWrapper stmt, int maxRetries, Counter retryCounter)
            throws SQLException {
        attemptExecuteBatch(stmt::executeBatch, maxRetries, retryCounter, () -> {});
    }

    default void attemptExecuteBatch(
            SqlBatchOperation operation,
            int maxRetries,
            Counter retryCounter,
            SqlBatchOperation beforeRetry)
            throws SQLException {
        for (int i = 0; i <= maxRetries; i++) {
            try {
                operation.execute();
                return;
            } catch (SQLException exception) {
                LOG.warn(
                        "ClickHouse executeBatch failed on attempt {} of {}",
                        i + 1,
                        maxRetries + 1,
                        exception);
                if (!isRetryable(exception)) {
                    throw new SQLException(
                            "ClickHouse batch failed with a non-retryable JDBC error", exception);
                }
                if (i >= maxRetries) {
                    throw new SQLException(
                            String.format(
                                    "Attempt to execute batch failed, exhausted retry times = %d",
                                    maxRetries),
                            exception);
                }
                if (retryCounter != null) {
                    retryCounter.inc();
                }
                try {
                    Thread.sleep(retryDelayMillis(i + 1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(
                            "Unable to flush; interrupted while doing another attempt", ex);
                }
                beforeRetry.execute();
            }
        }
    }

    /** A JDBC batch operation that may fail with a checked SQL exception. */
    @FunctionalInterface
    interface SqlBatchOperation {
        void execute() throws SQLException;
    }

    static long retryDelayMillis(int retryNumber) {
        long baseDelay = retryBaseDelayMillis(retryNumber);
        long jitter = Math.max(1L, baseDelay / 5L);
        return Math.min(
                30_000L,
                ThreadLocalRandom.current().nextLong(baseDelay - jitter, baseDelay + jitter + 1L));
    }

    static long retryBaseDelayMillis(int retryNumber) {
        int exponent = Math.min(Math.max(retryNumber - 1, 0), 5);
        return Math.min(1000L << exponent, 30_000L);
    }

    static boolean isRetryable(SQLException exception) {
        for (SQLException current = exception;
                current != null;
                current = current.getNextException()) {
            if (current instanceof SQLTransientException
                    || current instanceof SQLRecoverableException) {
                return true;
            }
            String sqlState = current.getSQLState();
            if (sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("40"))) {
                return true;
            }
        }
        return false;
    }

    static ClickHouseExecutor createClickHouseExecutor(
            String tableName,
            String databaseName,
            String clusterName,
            String[] fieldNames,
            String[] keyFields,
            String[] partitionFields,
            LogicalType[] fieldTypes,
            ClickHouseDmlOptions options) {
        if (keyFields.length > 0) {
            return createUpsertExecutor(
                    tableName,
                    databaseName,
                    clusterName,
                    fieldNames,
                    keyFields,
                    partitionFields,
                    fieldTypes,
                    options);
        } else {
            return createBatchExecutor(tableName, databaseName, fieldNames, fieldTypes, options);
        }
    }

    static ClickHouseBatchExecutor createBatchExecutor(
            String tableName,
            String databaseName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            ClickHouseDmlOptions options) {
        String insertSql =
                ClickHouseStatementFactory.getInsertIntoStatement(
                        tableName, databaseName, fieldNames);
        RowType rowType = RowType.of(fieldTypes);
        ClickHouseRowConverter converter = new ClickHouseRowConverter(rowType);
        RowDataSerializer rowSerializer = new RowDataSerializer(rowType);
        return new ClickHouseBatchExecutor(
                insertSql,
                converter,
                rowSerializer::copy,
                row -> rowSerializer.toBinaryRow(row).getSizeInBytes(),
                options);
    }

    static ClickHouseUpsertExecutor createUpsertExecutor(
            String tableName,
            String databaseName,
            String clusterName,
            String[] fieldNames,
            String[] keyFieldNames,
            String[] partitionFields,
            LogicalType[] fieldTypes,
            ClickHouseDmlOptions options) {
        String insertSql =
                ClickHouseStatementFactory.getInsertIntoStatement(
                        tableName, databaseName, fieldNames);
        String updateSql =
                ClickHouseStatementFactory.getUpdateStatement(
                        tableName,
                        databaseName,
                        clusterName,
                        fieldNames,
                        keyFieldNames,
                        partitionFields);
        String deleteSql =
                ClickHouseStatementFactory.getDeleteStatement(
                        tableName, databaseName, clusterName, keyFieldNames);

        // Re-sort the order of fields to fit the sql statement.
        int[] keyFields =
                Arrays.stream(keyFieldNames)
                        .mapToInt(pk -> ArrayUtils.indexOf(fieldNames, pk))
                        .toArray();
        int[] updatableFields =
                IntStream.range(0, fieldNames.length)
                        .filter(idx -> !ArrayUtils.contains(keyFieldNames, fieldNames[idx]))
                        .filter(idx -> !ArrayUtils.contains(partitionFields, fieldNames[idx]))
                        .toArray();
        int[] updFields = ArrayUtils.addAll(updatableFields, keyFields);

        LogicalType[] keyTypes =
                Arrays.stream(keyFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
        LogicalType[] updTypes =
                Arrays.stream(updFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
        RowDataSerializer rowSerializer = new RowDataSerializer(RowType.of(fieldTypes));

        return new ClickHouseUpsertExecutor(
                insertSql,
                updateSql,
                deleteSql,
                new ClickHouseRowConverter(RowType.of(fieldTypes)),
                new ClickHouseRowConverter(RowType.of(updTypes)),
                new ClickHouseRowConverter(RowType.of(keyTypes)),
                createExtractor(fieldTypes, updFields),
                createKeyExtractor(fieldTypes, keyFields),
                rowSerializer::copy,
                row -> rowSerializer.toBinaryRow(row).getSizeInBytes(),
                options);
    }

    static Function<RowData, RowData> createExtractor(LogicalType[] logicalTypes, int[] fields) {
        return createExtractor(logicalTypes, fields, true);
    }

    static Function<RowData, RowData> createKeyExtractor(LogicalType[] logicalTypes, int[] fields) {
        return createExtractor(logicalTypes, fields, false);
    }

    static Function<RowData, RowData> createExtractor(
            LogicalType[] logicalTypes, int[] fields, boolean preserveRowKind) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[fields[i]], fields[i]);
        }

        return row -> {
            GenericRowData rowData =
                    new GenericRowData(
                            preserveRowKind ? row.getRowKind() : RowKind.INSERT,
                            fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                rowData.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            return rowData;
        };
    }
}
