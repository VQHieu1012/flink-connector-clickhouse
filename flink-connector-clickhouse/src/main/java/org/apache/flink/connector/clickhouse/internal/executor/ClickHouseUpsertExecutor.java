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
import org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseStatementWrapper;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.DISCARD;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.INSERT;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.UPDATE;

/** ClickHouse's upsert executor. */
public class ClickHouseUpsertExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final ClickHouseRowConverter insertConverter;

    private final ClickHouseRowConverter updateConverter;

    private final ClickHouseRowConverter deleteConverter;

    private final Function<RowData, RowData> updateExtractor;

    private final Function<RowData, RowData> keyExtractor;

    private final Function<RowData, RowData> activeRowTransformer;

    private final Function<RowData, RowData> tombstoneTransformer;

    private final Function<RowData, RowData> rowCopier;

    private final ToLongFunction<RowData> rowSizeEstimator;

    private final int maxRetries;

    private final SinkUpdateStrategy updateStrategy;

    private final boolean ignoreDelete;

    private final Map<RowData, RowData> reduceBuffer = new LinkedHashMap<>();

    private final List<RowData> appendBuffer = new ArrayList<>();

    private final Map<RowData, Long> bufferedRecordSizes = new LinkedHashMap<>();

    private long bufferedBytes;

    private transient ClickHouseStatementWrapper insertStatement;

    private transient ClickHouseStatementWrapper updateStatement;

    private transient ClickHouseStatementWrapper deleteStatement;

    private transient ClickHouseConnectionProvider connectionProvider;

    private transient Counter retryCounter;

    public ClickHouseUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            ClickHouseRowConverter insertConverter,
            ClickHouseRowConverter updateConverter,
            ClickHouseRowConverter deleteConverter,
            Function<RowData, RowData> updateExtractor,
            Function<RowData, RowData> keyExtractor,
            Function<RowData, RowData> rowCopier,
            ToLongFunction<RowData> rowSizeEstimator,
            ClickHouseDmlOptions options) {
        this(
                insertSql,
                updateSql,
                deleteSql,
                insertConverter,
                updateConverter,
                deleteConverter,
                updateExtractor,
                keyExtractor,
                row -> row,
                row -> row,
                rowCopier,
                rowSizeEstimator,
                options);
    }

    public ClickHouseUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            ClickHouseRowConverter insertConverter,
            ClickHouseRowConverter updateConverter,
            ClickHouseRowConverter deleteConverter,
            Function<RowData, RowData> updateExtractor,
            Function<RowData, RowData> keyExtractor,
            Function<RowData, RowData> activeRowTransformer,
            Function<RowData, RowData> tombstoneTransformer,
            Function<RowData, RowData> rowCopier,
            ToLongFunction<RowData> rowSizeEstimator,
            ClickHouseDmlOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.insertConverter = insertConverter;
        this.updateConverter = updateConverter;
        this.deleteConverter = deleteConverter;
        this.updateExtractor = updateExtractor;
        this.keyExtractor = keyExtractor;
        this.activeRowTransformer = activeRowTransformer;
        this.tombstoneTransformer = tombstoneTransformer;
        this.rowCopier = rowCopier;
        this.rowSizeEstimator = rowSizeEstimator;
        this.maxRetries = options.getMaxRetries();
        this.updateStrategy = options.getUpdateStrategy();
        this.ignoreDelete = options.isIgnoreDelete();
    }

    @Override
    public void prepareStatement(Connection connection) throws SQLException {
        this.insertStatement =
                new ClickHouseStatementWrapper(connection.prepareStatement(this.insertSql));
        if (!INSERT.equals(updateStrategy)) {
            this.updateStatement =
                    new ClickHouseStatementWrapper(connection.prepareStatement(this.updateSql));
            this.deleteStatement =
                    new ClickHouseStatementWrapper(connection.prepareStatement(this.deleteSql));
        }
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getOrCreateConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public void setRetryCounter(Counter retryCounter) {
        this.retryCounter = retryCounter;
    }

    @Override
    public void addToBatch(RowData record) {
        // Flink may reuse RowData instances after write() returns. Retain an owned copy only.
        RowData recordCopy = rowCopier.apply(record);
        long recordSize = rowSizeEstimator.applyAsLong(recordCopy);
        if (INSERT.equals(updateStrategy)) {
            appendBuffer.add(recordCopy);
            bufferedBytes += recordSize;
            return;
        }

        RowData key = keyExtractor.apply(recordCopy);
        Long replacedSize = bufferedRecordSizes.put(key, recordSize);
        if (replacedSize != null) {
            bufferedBytes -= replacedSize;
        }
        bufferedBytes += recordSize;
        reduceBuffer.put(key, recordCopy);
    }

    int getBufferedRecordCount() {
        return INSERT.equals(updateStrategy) ? appendBuffer.size() : reduceBuffer.size();
    }

    @Override
    public long getBufferedBytes() {
        return bufferedBytes;
    }

    @Override
    public void executeBatch() throws SQLException {
        attemptExecuteBatch(
                this::bindAndExecuteBatch,
                maxRetries,
                retryCounter,
                this::reconnectAndPrepareStatements);
        reduceBuffer.clear();
        appendBuffer.clear();
        bufferedRecordSizes.clear();
        bufferedBytes = 0L;
    }

    private void bindAndExecuteBatch() throws SQLException {
        Iterable<RowData> records =
                INSERT.equals(updateStrategy) ? appendBuffer : reduceBuffer.values();
        for (RowData value : records) {
            addValueToBatch(value);
        }

        for (ClickHouseStatementWrapper clickHouseStatement :
                Arrays.asList(insertStatement, updateStatement, deleteStatement)) {
            if (clickHouseStatement != null) {
                clickHouseStatement.executeBatch();
            }
        }
    }

    private void reconnectAndPrepareStatements() throws SQLException {
        if (connectionProvider == null) {
            throw new SQLException("Cannot reconnect a ClickHouse executor without a provider");
        }
        closeStatement();
        prepareStatement(connectionProvider.reconnect());
    }

    private void addValueToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                insertConverter.toExternal(activeRowTransformer.apply(record), insertStatement);
                insertStatement.addBatch();
                break;
            case UPDATE_AFTER:
                if (INSERT.equals(updateStrategy)) {
                    insertConverter.toExternal(activeRowTransformer.apply(record), insertStatement);
                    insertStatement.addBatch();
                } else if (UPDATE.equals(updateStrategy)) {
                    updateConverter.toExternal(updateExtractor.apply(record), updateStatement);
                    updateStatement.addBatch();
                } else if (DISCARD.equals(updateStrategy)) {
                    LOG.debug("Discard a record of type UPDATE_AFTER: {}", record);
                } else {
                    throw new RuntimeException("Unknown update strategy: " + updateStrategy);
                }
                break;
            case DELETE:
                if (!ignoreDelete) {
                    if (INSERT.equals(updateStrategy)) {
                        insertConverter.toExternal(
                                tombstoneTransformer.apply(record), insertStatement);
                        insertStatement.addBatch();
                    } else {
                        deleteConverter.toExternal(keyExtractor.apply(record), deleteStatement);
                        deleteStatement.addBatch();
                    }
                }
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    @Override
    public void closeStatement() {
        for (ClickHouseStatementWrapper clickHouseStatement :
                Arrays.asList(insertStatement, updateStatement, deleteStatement)) {
            if (clickHouseStatement != null) {
                try {
                    clickHouseStatement.close();
                } catch (SQLException exception) {
                    LOG.warn("ClickHouse upsert statement could not be closed.", exception);
                }
            }
        }
        insertStatement = null;
        updateStatement = null;
        deleteStatement = null;
    }

    @Override
    public String toString() {
        return "ClickHouseUpsertExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", updateSql='"
                + updateSql
                + '\''
                + ", deleteSql='"
                + deleteSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", updateStrategy="
                + updateStrategy
                + ", ignoreDelete="
                + ignoreDelete
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
