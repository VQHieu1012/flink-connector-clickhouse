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

package org.apache.flink.connector.clickhouse.internal.options;

import org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkShardingStrategy;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.SinkUpdateStrategy;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.DEFAULT_CDC_DELETED_COLUMN;
import static org.apache.flink.connector.clickhouse.config.ClickHouseConfigOptions.DEFAULT_CDC_VERSION_COLUMN;

/** ClickHouse data modify language options. */
public class ClickHouseDmlOptions extends ClickHouseConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final int batchSize;

    private final long maxBufferedBytes;

    private final Duration flushInterval;

    private final int maxRetries;

    private final boolean useLocal;

    private final SinkUpdateStrategy updateStrategy;

    private final String cdcVersionColumn;

    private final String cdcDeletedColumn;

    private final SinkShardingStrategy shardingStrategy;

    private final List<String> shardingKey;

    private final boolean shardingUseTableDef;

    private final boolean ignoreDelete;

    private final Integer parallelism;

    private ClickHouseDmlOptions(
            String url,
            @Nullable String username,
            @Nullable String password,
            String databaseName,
            String tableName,
            int batchSize,
            long maxBufferedBytes,
            Duration flushInterval,
            int maxRetires,
            boolean useLocal,
            SinkUpdateStrategy updateStrategy,
            String cdcVersionColumn,
            String cdcDeletedColumn,
            SinkShardingStrategy shardingStrategy,
            List<String> shardingKey,
            boolean shardingUseTableDef,
            boolean ignoreDelete,
            Integer parallelism) {
        super(url, username, password, databaseName, tableName);
        this.batchSize = batchSize;
        this.maxBufferedBytes = maxBufferedBytes;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.useLocal = useLocal;
        this.updateStrategy = updateStrategy;
        this.cdcVersionColumn = cdcVersionColumn;
        this.cdcDeletedColumn = cdcDeletedColumn;
        this.shardingStrategy = shardingStrategy;
        this.shardingKey = shardingKey;
        this.shardingUseTableDef = shardingUseTableDef;
        this.ignoreDelete = ignoreDelete;
        this.parallelism = parallelism;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public long getMaxBufferedBytes() {
        return maxBufferedBytes;
    }

    public Duration getFlushInterval() {
        return this.flushInterval;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public boolean isUseLocal() {
        return this.useLocal;
    }

    public SinkUpdateStrategy getUpdateStrategy() {
        return updateStrategy;
    }

    public String getCdcVersionColumn() {
        return cdcVersionColumn;
    }

    public String getCdcDeletedColumn() {
        return cdcDeletedColumn;
    }

    public SinkShardingStrategy getShardingStrategy() {
        return this.shardingStrategy;
    }

    public List<String> getShardingKey() {
        return this.shardingKey;
    }

    public boolean isShardingUseTableDef() {
        return shardingUseTableDef;
    }

    public boolean isIgnoreDelete() {
        return this.ignoreDelete;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    /** Builder for {@link ClickHouseDmlOptions}. */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String databaseName;
        private String tableName;
        private int batchSize;
        private long maxBufferedBytes = Long.MAX_VALUE;
        private Duration flushInterval;
        private int maxRetries;
        private boolean useLocal;
        private SinkUpdateStrategy updateStrategy;
        private String cdcVersionColumn = DEFAULT_CDC_VERSION_COLUMN;
        private String cdcDeletedColumn = DEFAULT_CDC_DELETED_COLUMN;
        private SinkShardingStrategy shardingStrategy;
        private List<String> shardingKey;
        private boolean shardingUseTableDef;
        private boolean ignoreDelete;
        private Integer parallelism;

        public Builder() {}

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withMaxBufferedBytes(long maxBufferedBytes) {
            this.maxBufferedBytes = maxBufferedBytes;
            return this;
        }

        public Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder withUpdateStrategy(SinkUpdateStrategy updateStrategy) {
            this.updateStrategy = updateStrategy;
            return this;
        }

        public Builder withCdcVersionColumn(String cdcVersionColumn) {
            this.cdcVersionColumn = cdcVersionColumn;
            return this;
        }

        public Builder withCdcDeletedColumn(String cdcDeletedColumn) {
            this.cdcDeletedColumn = cdcDeletedColumn;
            return this;
        }

        public Builder withUseLocal(Boolean useLocal) {
            this.useLocal = useLocal;
            return this;
        }

        public Builder withShardingStrategy(SinkShardingStrategy shardingStrategy) {
            this.shardingStrategy = shardingStrategy;
            return this;
        }

        public Builder withShardingKey(String shardingKey) {
            this.shardingKey =
                    shardingKey == null
                            ? Collections.emptyList()
                            : Collections.singletonList(shardingKey);
            return this;
        }

        public Builder withUseTableDef(boolean shardingUseTableDef) {
            this.shardingUseTableDef = shardingUseTableDef;
            return this;
        }

        public Builder withIgnoreDelete(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
            return this;
        }

        public Builder withParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public ClickHouseDmlOptions build() {
            return new ClickHouseDmlOptions(
                    url,
                    username,
                    password,
                    databaseName,
                    tableName,
                    batchSize,
                    maxBufferedBytes,
                    flushInterval,
                    maxRetries,
                    useLocal,
                    updateStrategy,
                    cdcVersionColumn,
                    cdcDeletedColumn,
                    shardingStrategy,
                    shardingKey,
                    shardingUseTableDef,
                    ignoreDelete,
                    parallelism);
        }
    }
}
