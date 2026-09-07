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

package org.apache.flink.connector.clickhouse.internal.converter;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseStatementWrapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Struct;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests conversion of nested ClickHouse Tuple values represented as Flink ROW fields. */
public class ClickHouseRowConverterTest {

    private static final RowType NESTED_TYPE =
            RowType.of(new IntType(), new VarCharType(), new IntType());
    private static final RowType ROW_TYPE = RowType.of(NESTED_TYPE);

    @Test
    public void serializesRowFieldAsTupleValues() throws Exception {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ClickHouseRowConverter converter = new ClickHouseRowConverter(ROW_TYPE);
        GenericRowData nested = GenericRowData.of(42, StringData.fromString("nested"), null);

        converter.toExternal(
                GenericRowData.of(nested), new ClickHouseStatementWrapper(preparedStatement));

        ArgumentCaptor<Object> tupleCaptor = ArgumentCaptor.forClass(Object.class);
        verify(preparedStatement)
                .setObject(org.mockito.ArgumentMatchers.eq(1), tupleCaptor.capture());
        Struct tuple = (Struct) tupleCaptor.getValue();
        assertEquals(Arrays.asList(42, "nested", null), Arrays.asList(tuple.getAttributes()));
    }

    @Test
    public void deserializesTupleValuesAsRowField() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(Arrays.asList(42, "nested", null));

        assertConvertedRow(resultSet);
    }

    @Test
    public void deserializesJdbcV2ArrayTupleAsRowField() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(new Object[] {42, "nested", null});

        assertConvertedRow(resultSet);
    }

    private void assertConvertedRow(ResultSet resultSet) throws Exception {
        ClickHouseRowConverter converter = new ClickHouseRowConverter(ROW_TYPE);

        RowData result = converter.toInternal(resultSet);
        RowData nested = result.getRow(0, NESTED_TYPE.getFieldCount());

        assertEquals(42, nested.getInt(0));
        assertEquals("nested", nested.getString(1).toString());
        assertTrue(nested.isNullAt(2));
    }
}
