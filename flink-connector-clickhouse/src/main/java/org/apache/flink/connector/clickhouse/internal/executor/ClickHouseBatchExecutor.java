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
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/** ClickHouse's batch executor. */
public class ClickHouseBatchExecutor implements ClickHouseExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private final String insertSql;

    private final ClickHouseRowConverter converter;

    private final Function<RowData, RowData> rowCopier;

    private final ToLongFunction<RowData> rowSizeEstimator;

    private final int maxRetries;

    private transient ClickHouseStatementWrapper statement;

    private transient ClickHouseConnectionProvider connectionProvider;

    private transient Counter retryCounter;

    private final List<RowData> batch = new ArrayList<>();

    private long bufferedBytes;

    public ClickHouseBatchExecutor(
            String insertSql,
            ClickHouseRowConverter converter,
            Function<RowData, RowData> rowCopier,
            ToLongFunction<RowData> rowSizeEstimator,
            ClickHouseDmlOptions options) {
        this.insertSql = insertSql;
        this.converter = converter;
        this.rowCopier = rowCopier;
        this.rowSizeEstimator = rowSizeEstimator;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareStatement(Connection connection) throws SQLException {
        statement = new ClickHouseStatementWrapper(connection.prepareStatement(insertSql));
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
    public void addToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                RowData ownedRecord = rowCopier.apply(record);
                batch.add(ownedRecord);
                bufferedBytes += rowSizeEstimator.applyAsLong(ownedRecord);
                break;
            case UPDATE_AFTER:
            case DELETE:
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
    public void executeBatch() throws SQLException {
        attemptExecuteBatch(
                this::bindAndExecuteBatch,
                maxRetries,
                retryCounter,
                this::reconnectAndPrepareStatement);
        batch.clear();
        bufferedBytes = 0L;
    }

    @Override
    public long getBufferedBytes() {
        return bufferedBytes;
    }

    private void bindAndExecuteBatch() throws SQLException {
        for (RowData record : batch) {
            converter.toExternal(record, statement);
            statement.addBatch();
        }
        statement.executeBatch();
    }

    private void reconnectAndPrepareStatement() throws SQLException {
        if (connectionProvider == null) {
            throw new SQLException("Cannot reconnect a ClickHouse executor without a provider");
        }
        closeStatement();
        prepareStatement(connectionProvider.reconnect());
    }

    @Override
    public void closeStatement() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException exception) {
                LOG.warn("ClickHouse batch statement could not be closed.", exception);
            } finally {
                statement = null;
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseBatchExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
