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
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import org.junit.Test;

import java.sql.SQLTransientException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests append buffering and connection recovery. */
public class ClickHouseBatchExecutorTest {

    @Test
    public void reconnectsAndReplaysOwnedRowsAfterTransientFailure() throws Exception {
        RowType rowType = RowType.of(new BigIntType(), new VarCharType());
        RowDataSerializer serializer = new RowDataSerializer(rowType);
        ClickHouseDmlOptions options = new ClickHouseDmlOptions.Builder().withMaxRetries(1).build();
        ClickHouseBatchExecutor executor =
                new ClickHouseBatchExecutor(
                        "insert",
                        new ClickHouseRowConverter(rowType),
                        serializer::copy,
                        row -> serializer.toBinaryRow(row).getSizeInBytes(),
                        options);
        SimpleCounter retries = new SimpleCounter();
        executor.setRetryCounter(retries);

        ClickHousePreparedStatement firstStatement = mock(ClickHousePreparedStatement.class);
        ClickHousePreparedStatement secondStatement = mock(ClickHousePreparedStatement.class);
        when(firstStatement.executeBatch()).thenThrow(new SQLTransientException("connection lost"));
        ClickHouseConnection firstConnection = mock(ClickHouseConnection.class);
        ClickHouseConnection secondConnection = mock(ClickHouseConnection.class);
        when(firstConnection.prepareStatement("insert")).thenReturn(firstStatement);
        when(secondConnection.prepareStatement("insert")).thenReturn(secondStatement);
        ClickHouseConnectionProvider provider = mock(ClickHouseConnectionProvider.class);
        when(provider.getOrCreateConnection()).thenReturn(firstConnection);
        when(provider.reconnect()).thenReturn(secondConnection);
        executor.prepareStatement(provider);

        GenericRowData reused = GenericRowData.of(1L, StringData.fromString("original"));
        executor.addToBatch(reused);
        assertTrue(executor.getBufferedBytes() > 0L);
        reused.setField(0, 99L);
        reused.setField(1, StringData.fromString("mutated"));

        executor.executeBatch();

        verify(provider).reconnect();
        verify(secondStatement).setLong(1, 1L);
        verify(secondStatement).setString(2, "original");
        verify(secondStatement).executeBatch();
        assertEquals(1L, retries.getCount());
        assertEquals(0L, executor.getBufferedBytes());
    }
}
