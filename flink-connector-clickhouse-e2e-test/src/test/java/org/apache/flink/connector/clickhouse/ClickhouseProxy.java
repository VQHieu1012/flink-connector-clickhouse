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

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.junit.Assert;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** A proxy for Clickhouse to execute SQLs and check results. */
public class ClickhouseProxy implements AutoCloseable {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    Statement statement;
    Connection connection;

    ClickhouseProxy(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void connect() {
        try {
            if (connection == null || connection.isClosed()) {
                Properties properties = new Properties();
                properties.put("compress", "false");
                properties.put("decompress", "false");
                ClickHouseDataSource clickHouseDataSource =
                        new ClickHouseDataSource(jdbcUrl, properties);
                connection = clickHouseDataSource.getConnection(username, password);
                statement = connection.createStatement();
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot establish ClickHouse connection", e);
        }
    }

    public void execute(String sql) throws SQLException {
        connect();
        statement.execute(sql);
    }

    private void checkResult(List<String> expectedResult, String table, List<String> fields)
            throws Exception {
        connect();
        List<String> results = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery("select * from " + table)) {
            while (resultSet.next()) {
                List<String> result = new ArrayList<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    if (!fields.contains(columnName)) {
                        continue;
                    }
                    String columnType = metaData.getColumnTypeName(i);
                    if (columnType.startsWith("Array")) {
                        Array array = resultSet.getArray(i);
                        result.add(
                                Arrays.deepToString((Object[]) array.getArray()).replace(" ", ""));
                        continue;
                    }
                    switch (columnType) {
                        case "Timestamp":
                            Timestamp timestamp = resultSet.getTimestamp(i);
                            result.add(timestamp.toString());
                            break;
                        default:
                            String value = resultSet.getString(i);
                            result.add(value);
                            break;
                    }
                }

                results.add(String.join(",", result));
            }
        }
        Collections.sort(results);
        List<String> sortedExpected = new ArrayList<>(expectedResult);
        Collections.sort(sortedExpected);
        Assert.assertArrayEquals(sortedExpected.toArray(), results.toArray());
    }

    public void checkResultWithTimeout(
            List<String> expectedResult, String table, List<String> fields, long timeout)
            throws Exception {
        long endTimeout = System.currentTimeMillis() + timeout;
        boolean result = false;
        while (System.currentTimeMillis() < endTimeout) {
            try {
                checkResult(expectedResult, table, fields);
                result = true;
                break;
            } catch (AssertionError | SQLException throwable) {
                Thread.sleep(1000L);
            }
        }
        if (!result) {
            checkResult(expectedResult, table, fields);
        }
    }

    public long waitForRowCountAtLeast(String table, long minimum, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        long count = 0L;
        while (System.currentTimeMillis() < deadline) {
            count = getRowCount(table);
            if (count >= minimum) {
                return count;
            }
            Thread.sleep(500L);
        }
        Assert.fail(
                String.format(
                        "Expected at least %d rows in %s, but found %d", minimum, table, count));
        return count;
    }

    private long getRowCount(String table) throws SQLException {
        connect();
        try (ResultSet resultSet = statement.executeQuery("select count() from " + table)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    @Override
    public void close() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException ignored) {
                // Best-effort cleanup in test code.
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // Best-effort cleanup in test code.
            }
        }
    }
}
