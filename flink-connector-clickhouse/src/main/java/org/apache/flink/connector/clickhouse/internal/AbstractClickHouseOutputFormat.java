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

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkShardingStrategy;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.schema.ClusterSpec;
import org.apache.flink.connector.clickhouse.internal.schema.DistributedEngineFull;
import org.apache.flink.connector.clickhouse.internal.schema.Expression;
import org.apache.flink.connector.clickhouse.internal.schema.FieldExpr;
import org.apache.flink.connector.clickhouse.internal.schema.FunctionExpr;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.connector.clickhouse.util.ClickHouseJdbcUtil.getClusterSpec;
import static org.apache.flink.connector.clickhouse.util.ClickHouseJdbcUtil.getDistributedEngineFull;

/** Abstract class of ClickHouse output format. */
public abstract class AbstractClickHouseOutputFormat extends RichOutputFormat<RowData>
        implements Flushable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClickHouseOutputFormat.class);

    protected transient volatile boolean closed = false;

    protected transient ScheduledExecutorService scheduler;

    protected transient ScheduledFuture<?> scheduledFuture;

    protected transient volatile Exception flushException;

    private transient Counter numRecordsSendCounter;

    private transient Counter numRecordsSendErrorsCounter;

    private transient Counter numBytesSendCounter;

    private transient Counter numBatchesSendCounter;

    private transient Counter numBatchesSendErrorsCounter;

    private transient Counter numRetriesCounter;

    private transient AtomicLong currentSendStartNanos;

    public AbstractClickHouseOutputFormat() {}

    protected abstract void open() throws IOException;

    @Override
    public void configure(Configuration parameters) {}

    void initializeMetrics(SinkWriterMetricGroup metricGroup) {
        numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        numRecordsSendErrorsCounter = metricGroup.getNumRecordsSendErrorsCounter();
        numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        MetricGroup clickHouseMetrics = metricGroup.addGroup("clickhouse");
        numBatchesSendCounter = clickHouseMetrics.counter("batchesSent");
        numBatchesSendErrorsCounter = clickHouseMetrics.counter("batchesSendErrors");
        numRetriesCounter = clickHouseMetrics.counter("retries");
        clickHouseMetrics.gauge("bufferedRecords", this::getBufferedRecordCount);
        clickHouseMetrics.gauge("bufferedBytes", this::getBufferedBytes);
        currentSendStartNanos = new AtomicLong();
        metricGroup.setCurrentSendTimeGauge(
                () -> {
                    long startNanos = currentSendStartNanos.get();
                    return startNanos == 0L
                            ? 0L
                            : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                });
    }

    protected Counter getNumRetriesCounter() {
        return numRetriesCounter;
    }

    protected long getBufferedRecordCount() {
        return 0L;
    }

    protected long getBufferedBytes() {
        return 0L;
    }

    public void scheduledFlush(long intervalMillis, String executorName) {
        Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
        scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
        scheduledFuture =
                scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        flushException = e;
                                    }
                                }
                            }
                        },
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    public void checkBeforeFlush(final ClickHouseExecutor executor, long pendingRecords)
            throws IOException {
        checkFlushException();
        long pendingBytes = executor.getBufferedBytes();
        if (currentSendStartNanos != null) {
            currentSendStartNanos.set(System.nanoTime());
        }
        try {
            executor.executeBatch();
            increment(numRecordsSendCounter, pendingRecords);
            increment(numBytesSendCounter, pendingBytes);
            increment(numBatchesSendCounter, 1L);
        } catch (Exception e) {
            increment(numRecordsSendErrorsCounter, pendingRecords);
            increment(numBatchesSendErrorsCounter, 1L);
            throw new IOException(e);
        } finally {
            if (currentSendStartNanos != null) {
                currentSendStartNanos.set(0L);
            }
        }
    }

    private static void increment(Counter counter, long value) {
        if (counter != null) {
            counter.inc(value);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            IOException failure = null;
            try {
                checkFlushException();
                flush();
            } catch (Exception exception) {
                failure = asIOException("Flushing records to ClickHouse failed.", exception);
            } finally {
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(false);
                }
                if (scheduler != null) {
                    scheduler.shutdown();
                }

                try {
                    closeOutputFormat();
                } catch (Exception exception) {
                    IOException closeFailure =
                            asIOException("Closing ClickHouse output format failed.", exception);
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
    }

    protected void checkFlushException() throws IOException {
        if (flushException != null) {
            throw asIOException("Asynchronous ClickHouse flush failed.", flushException);
        }
    }

    private IOException asIOException(String message, Exception exception) {
        return exception instanceof IOException
                ? (IOException) exception
                : new IOException(message, exception);
    }

    protected abstract void closeOutputFormat();

    /** Builder for {@link ClickHouseBatchOutputFormat} and {@link ClickHouseShardOutputFormat}. */
    public static class Builder {

        private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

        private DataType[] fieldTypes;

        private LogicalType[] logicalTypes;

        private ClickHouseDmlOptions options;

        private Properties connectionProperties;

        private String[] fieldNames;

        private String[] primaryKeys;

        private String[] partitionKeys;

        public Builder() {}

        public Builder withOptions(ClickHouseDmlOptions options) {
            this.options = options;
            return this;
        }

        public Builder withConnectionProperties(Properties connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public Builder withFieldTypes(DataType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            this.logicalTypes =
                    Arrays.stream(fieldTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            return this;
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withPrimaryKey(String[] primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public Builder withPartitionKey(String[] partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public AbstractClickHouseOutputFormat build() {
            Preconditions.checkNotNull(options);
            Preconditions.checkNotNull(fieldNames);
            Preconditions.checkNotNull(fieldTypes);
            Preconditions.checkNotNull(primaryKeys);
            Preconditions.checkNotNull(partitionKeys);
            if (primaryKeys.length > 0) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn(
                        "The data will be updated / deleted by the primary key, you will have significant performance loss.");
            }

            ClickHouseConnectionProvider connectionProvider = null;
            try {
                connectionProvider =
                        new ClickHouseConnectionProvider(options, connectionProperties);
                DistributedEngineFull engineFullSchema =
                        getDistributedEngineFull(
                                connectionProvider.getOrCreateConnection(),
                                options.getDatabaseName(),
                                options.getTableName());

                boolean isDistributed = engineFullSchema != null;
                return isDistributed && options.isUseLocal()
                        ? createShardOutputFormat(
                                connectionProvider.getOrCreateConnection(), engineFullSchema)
                        : createBatchOutputFormat();
            } catch (Exception exception) {
                throw new RuntimeException("Build ClickHouse output format failed.", exception);
            } finally {
                if (connectionProvider != null) {
                    connectionProvider.closeConnections();
                }
            }
        }

        private ClickHouseBatchOutputFormat createBatchOutputFormat() {
            return new ClickHouseBatchOutputFormat(
                    new ClickHouseConnectionProvider(options, connectionProperties),
                    fieldNames,
                    primaryKeys,
                    partitionKeys,
                    logicalTypes,
                    options);
        }

        private ClickHouseShardOutputFormat createShardOutputFormat(
                Connection connection, DistributedEngineFull engineFullSchema) throws SQLException {
            SinkShardingStrategy shardingStrategy;
            List<FieldGetter> fieldGetters = null;
            if (options.isShardingUseTableDef()) {
                Expression shardingKey = engineFullSchema.getShardingKey();
                if (shardingKey instanceof FieldExpr) {
                    shardingStrategy = SinkShardingStrategy.VALUE;
                    FieldGetter fieldGetter =
                            getFieldGetterOfShardingKey(((FieldExpr) shardingKey).getColumnName());
                    fieldGetters = singletonList(fieldGetter);
                } else if (shardingKey instanceof FunctionExpr
                        && "rand()".equals(shardingKey.explain())) {
                    shardingStrategy = SinkShardingStrategy.SHUFFLE;
                    fieldGetters = emptyList();
                } else if (shardingKey instanceof FunctionExpr
                        && "javaHash".equals(((FunctionExpr) shardingKey).getFunctionName())
                        && ((FunctionExpr) shardingKey)
                                .getArguments().stream()
                                        .allMatch(expression -> expression instanceof FieldExpr)) {
                    shardingStrategy = SinkShardingStrategy.HASH;
                    fieldGetters = parseFieldGetters((FunctionExpr) shardingKey);
                } else {
                    throw new RuntimeException(
                            "Unsupported sharding key: " + shardingKey.explain());
                }
            } else {
                shardingStrategy = options.getShardingStrategy();
                if (shardingStrategy.shardingKeyNeeded) {
                    fieldGetters =
                            options.getShardingKey().stream()
                                    .map(this::getFieldGetterOfShardingKey)
                                    .collect(toList());
                }
            }

            ClusterSpec clusterSpec = getClusterSpec(connection, engineFullSchema.getCluster());
            return new ClickHouseShardOutputFormat(
                    new ClickHouseConnectionProvider(options, connectionProperties),
                    clusterSpec,
                    engineFullSchema,
                    fieldNames,
                    primaryKeys,
                    partitionKeys,
                    logicalTypes,
                    shardingStrategy.provider.apply(fieldGetters),
                    options);
        }

        private List<FieldGetter> parseFieldGetters(FunctionExpr functionExpr) {
            return functionExpr.getArguments().stream()
                    .map(
                            expression -> {
                                if (expression instanceof FunctionExpr) {
                                    return parseFieldGetters((FunctionExpr) expression);
                                } else {
                                    FieldGetter fieldGetter =
                                            getFieldGetterOfShardingKey(
                                                    ((FieldExpr) expression).getColumnName());
                                    return singletonList(fieldGetter);
                                }
                            })
                    .flatMap(Collection::stream)
                    .collect(toList());
        }

        private FieldGetter getFieldGetterOfShardingKey(String shardingKey) {
            int index = Arrays.asList(fieldNames).indexOf(shardingKey);
            if (index == -1) {
                throw new IllegalArgumentException(
                        String.format("Sharding key `%s` not found in table schema", shardingKey));
            }
            return RowData.createFieldGetter(logicalTypes[index], index);
        }
    }
}
