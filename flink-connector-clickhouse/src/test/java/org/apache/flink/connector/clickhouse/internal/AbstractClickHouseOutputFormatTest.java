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

package org.apache.flink.connector.clickhouse.internal;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.clickhouse.internal.executor.ClickHouseExecutor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for output-format failure propagation and Sink V2 flushing. */
public class AbstractClickHouseOutputFormatTest {

    @Test
    public void closePropagatesFlushFailureAndStillReleasesResources() {
        TestingOutputFormat format = new TestingOutputFormat();
        format.flushFailure = new IOException("expected flush failure");

        IOException failure = assertThrows(IOException.class, format::close);

        assertEquals("expected flush failure", failure.getMessage());
        assertTrue(format.closeCalled);
    }

    @Test
    public void sinkWriterFlushDelegatesOnCheckpoint() throws Exception {
        TestingOutputFormat format = new TestingOutputFormat();
        ClickHouseRowDataSinkWriter writer =
                new ClickHouseRowDataSinkWriter(writerContextWithMetrics(), format);

        writer.flush(false);

        assertEquals(1, format.flushCount);
        writer.close();
    }

    @Test
    public void reportsSuccessfulAndFailedFlushMetrics() throws Exception {
        TestingOutputFormat format = new TestingOutputFormat();
        SinkWriterMetricGroup metricGroup = mock(SinkWriterMetricGroup.class);
        MetricGroup clickHouseMetrics = mock(MetricGroup.class);
        SimpleCounter recordsSent = new SimpleCounter();
        SimpleCounter recordErrors = new SimpleCounter();
        SimpleCounter batchesSent = new SimpleCounter();
        SimpleCounter batchErrors = new SimpleCounter();
        when(metricGroup.getNumRecordsSendCounter()).thenReturn(recordsSent);
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(recordErrors);
        when(metricGroup.addGroup("clickhouse")).thenReturn(clickHouseMetrics);
        when(clickHouseMetrics.counter("batchesSent")).thenReturn(batchesSent);
        when(clickHouseMetrics.counter("batchesSendErrors")).thenReturn(batchErrors);
        when(clickHouseMetrics.counter("retries")).thenReturn(new SimpleCounter());
        format.initializeMetrics(metricGroup);
        ClickHouseExecutor executor = mock(ClickHouseExecutor.class);

        format.checkBeforeFlush(executor, 3L);
        doThrow(new java.sql.SQLException("expected")).when(executor).executeBatch();
        assertThrows(IOException.class, () -> format.checkBeforeFlush(executor, 2L));

        assertEquals(3L, recordsSent.getCount());
        assertEquals(2L, recordErrors.getCount());
        assertEquals(1L, batchesSent.getCount());
        assertEquals(1L, batchErrors.getCount());
    }

    private static WriterInitContext writerContextWithMetrics() {
        WriterInitContext context = mock(WriterInitContext.class);
        SinkWriterMetricGroup metricGroup = mock(SinkWriterMetricGroup.class);
        MetricGroup clickHouseMetrics = mock(MetricGroup.class);
        when(context.metricGroup()).thenReturn(metricGroup);
        when(metricGroup.getNumRecordsSendCounter()).thenReturn(new SimpleCounter());
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(new SimpleCounter());
        when(metricGroup.addGroup("clickhouse")).thenReturn(clickHouseMetrics);
        when(clickHouseMetrics.counter("batchesSent")).thenReturn(new SimpleCounter());
        when(clickHouseMetrics.counter("batchesSendErrors")).thenReturn(new SimpleCounter());
        when(clickHouseMetrics.counter("retries")).thenReturn(new SimpleCounter());
        return context;
    }

    private static final class TestingOutputFormat extends AbstractClickHouseOutputFormat {
        private IOException flushFailure;
        private int flushCount;
        private boolean closeCalled;

        @Override
        protected void open() {}

        @Override
        public void open(InitializationContext initializationContext) {}

        @Override
        public void writeRecord(RowData record) {}

        @Override
        public void flush() throws IOException {
            flushCount++;
            if (flushFailure != null) {
                throw flushFailure;
            }
        }

        @Override
        protected void closeOutputFormat() {
            closeCalled = true;
        }
    }
}
