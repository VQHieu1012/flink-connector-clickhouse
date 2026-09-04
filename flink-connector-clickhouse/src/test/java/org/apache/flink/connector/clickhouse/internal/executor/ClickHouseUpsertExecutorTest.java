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

package org.apache.flink.connector.clickhouse.internal.executor;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseStatementWrapper;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLTransientException;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy.INSERT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for upsert buffering semantics. */
public class ClickHouseUpsertExecutorTest {

    private static final LogicalType[] FIELD_TYPES =
            new LogicalType[] {new BigIntType(), new VarCharType()};

    @Test
    public void reducesDifferentRowKindsByPrimaryKey() {
        ClickHouseUpsertExecutor executor = createExecutor();

        executor.addToBatch(row(RowKind.INSERT, 1L, "before"));
        long firstRecordSize = executor.getBufferedBytes();
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 1L, "second"));

        assertEquals(1, executor.getBufferedRecordCount());
        assertEquals(firstRecordSize, executor.getBufferedBytes());
    }

    @Test
    public void usesBoundedExponentialRetryDelay() {
        assertEquals(1_000L, ClickHouseExecutor.retryBaseDelayMillis(1));
        assertEquals(2_000L, ClickHouseExecutor.retryBaseDelayMillis(2));
        assertEquals(30_000L, ClickHouseExecutor.retryBaseDelayMillis(100));
        assertTrue(ClickHouseExecutor.retryDelayMillis(1) >= 800L);
        assertTrue(ClickHouseExecutor.retryDelayMillis(1) <= 1_200L);
        assertTrue(ClickHouseExecutor.retryDelayMillis(100) <= 30_000L);
    }

    @Test
    public void retriesTransientBatchFailureAndReportsRetryMetric() throws Exception {
        ClickHouseExecutor executor = mock(ClickHouseExecutor.class, CALLS_REAL_METHODS);
        ClickHouseStatementWrapper statement = mock(ClickHouseStatementWrapper.class);
        SimpleCounter retries = new SimpleCounter();
        when(statement.executeBatch())
                .thenThrow(new SQLTransientException("temporary"))
                .thenReturn(new int[0]);

        executor.attemptExecuteBatch(statement, 1, retries);

        verify(statement, times(2)).executeBatch();
        assertEquals(1L, retries.getCount());
    }

    @Test
    public void doesNotRetryPermanentBatchFailure() throws Exception {
        ClickHouseExecutor executor = mock(ClickHouseExecutor.class, CALLS_REAL_METHODS);
        ClickHouseStatementWrapper statement = mock(ClickHouseStatementWrapper.class);
        SimpleCounter retries = new SimpleCounter();
        when(statement.executeBatch()).thenThrow(new SQLException("invalid data", "22000"));

        assertThrows(SQLException.class, () -> executor.attemptExecuteBatch(statement, 3, retries));

        verify(statement).executeBatch();
        assertEquals(0L, retries.getCount());
        assertTrue(ClickHouseExecutor.isRetryable(new SQLException("connection", "08006")));
        assertFalse(ClickHouseExecutor.isRetryable(new SQLException("data", "22000")));
    }

    @Test
    public void copiesRowsRetainedPastWriteCall() throws Exception {
        ClickHousePreparedStatement insertStatement = mock(ClickHousePreparedStatement.class);
        ClickHousePreparedStatement updateStatement = mock(ClickHousePreparedStatement.class);
        ClickHousePreparedStatement deleteStatement = mock(ClickHousePreparedStatement.class);
        ClickHouseConnection connection = mock(ClickHouseConnection.class);
        when(connection.prepareStatement("insert")).thenReturn(insertStatement);
        when(connection.prepareStatement("update")).thenReturn(updateStatement);
        when(connection.prepareStatement("delete")).thenReturn(deleteStatement);

        ClickHouseUpsertExecutor executor = createExecutor();
        executor.prepareStatement(connection);
        GenericRowData reused = row(RowKind.INSERT, 1L, "original");
        executor.addToBatch(reused);

        reused.setField(0, 99L);
        reused.setField(1, StringData.fromString("mutated"));
        executor.executeBatch();

        verify(insertStatement).setLong(1, 1L);
        verify(insertStatement).setString(2, "original");
    }

    @Test
    public void reconnectsAndReplaysUpsertBufferAfterTransientFailure() throws Exception {
        ClickHousePreparedStatement firstInsert = mock(ClickHousePreparedStatement.class);
        when(firstInsert.executeBatch()).thenThrow(new SQLTransientException("connection lost"));
        ClickHouseConnection firstConnection = connection(firstInsert);
        ClickHousePreparedStatement secondInsert = mock(ClickHousePreparedStatement.class);
        ClickHouseConnection secondConnection = connection(secondInsert);
        ClickHouseConnectionProvider provider = mock(ClickHouseConnectionProvider.class);
        when(provider.getOrCreateConnection()).thenReturn(firstConnection);
        when(provider.reconnect()).thenReturn(secondConnection);

        ClickHouseUpsertExecutor executor = createExecutor(1);
        executor.prepareStatement(provider);
        executor.addToBatch(row(RowKind.INSERT, 1L, "original"));

        executor.executeBatch();

        verify(provider).reconnect();
        verify(secondInsert).setLong(1, 1L);
        verify(secondInsert).setString(2, "original");
        verify(secondInsert).executeBatch();
    }

    private static ClickHouseConnection connection(ClickHousePreparedStatement insert)
            throws Exception {
        ClickHouseConnection connection = mock(ClickHouseConnection.class);
        when(connection.prepareStatement("insert")).thenReturn(insert);
        when(connection.prepareStatement("update"))
                .thenReturn(mock(ClickHousePreparedStatement.class));
        when(connection.prepareStatement("delete"))
                .thenReturn(mock(ClickHousePreparedStatement.class));
        return connection;
    }

    private static ClickHouseUpsertExecutor createExecutor() {
        return createExecutor(0);
    }

    private static ClickHouseUpsertExecutor createExecutor(int maxRetries) {
        RowType rowType = RowType.of(FIELD_TYPES);
        RowDataSerializer serializer = new RowDataSerializer(rowType);
        ClickHouseDmlOptions options =
                new ClickHouseDmlOptions.Builder()
                        .withMaxRetries(maxRetries)
                        .withUpdateStrategy(INSERT)
                        .withIgnoreDelete(false)
                        .build();

        return new ClickHouseUpsertExecutor(
                "insert",
                "update",
                "delete",
                new ClickHouseRowConverter(rowType),
                new ClickHouseRowConverter(rowType),
                new ClickHouseRowConverter(RowType.of(new BigIntType())),
                row -> row,
                ClickHouseExecutor.createKeyExtractor(FIELD_TYPES, new int[] {0}),
                serializer::copy,
                row -> serializer.toBinaryRow(row).getSizeInBytes(),
                options);
    }

    private static GenericRowData row(RowKind kind, long id, String value) {
        return GenericRowData.ofKind(kind, id, StringData.fromString(value));
    }
}
