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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Watches embedded Python calls that run synchronously through PemJa on the stream task thread.
 *
 * <p>Java interruption cannot reliably break a native PemJa frame. The watchdog therefore reports a
 * task failure from a separate daemon thread when a call overstays its timeout. If the native call
 * later returns, closing the watchdog rethrows the timeout on the task thread as well.
 */
@Internal
final class EmbeddedPythonOperationWatchdog implements AutoCloseable {

    private static final ScheduledExecutorService WATCHDOG_EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("EmbeddedPythonOperationWatchdog"));

    private static final long MAX_CHECK_INTERVAL_MILLIS = 1000L;

    private final Duration timeout;
    private final long timeoutMillis;
    private final long timeoutNanos;
    private final Consumer<Throwable> timeoutHandler;
    private final Logger log;
    private final AtomicReference<WatchedOperation> currentOperation = new AtomicReference<>();
    private final ScheduledFuture<?> timeoutFuture;

    EmbeddedPythonOperationWatchdog(
            Duration timeout, Consumer<Throwable> timeoutHandler, Logger log) {
        this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
        this.timeoutMillis = Math.max(1L, timeout.toMillis());
        this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        this.timeoutHandler =
                Objects.requireNonNull(timeoutHandler, "timeoutHandler must not be null");
        this.log = Objects.requireNonNull(log, "log must not be null");
        if (isEnabled()) {
            long checkIntervalMillis = Math.min(MAX_CHECK_INTERVAL_MILLIS, timeoutMillis);
            this.timeoutFuture =
                    WATCHDOG_EXECUTOR.scheduleWithFixedDelay(
                            this::checkCurrentOperation,
                            checkIntervalMillis,
                            checkIntervalMillis,
                            TimeUnit.MILLISECONDS);
        } else {
            this.timeoutFuture = null;
        }
    }

    AutoCloseable watch(String operationName) {
        if (!isEnabled()) {
            return () -> {};
        }

        WatchedOperation operation =
                new WatchedOperation(operationName, timeout, timeoutNanos, System.nanoTime());
        if (!currentOperation.compareAndSet(null, operation)) {
            throw new IllegalStateException(
                    "Embedded Python operation watchdog does not support nested operations.");
        }

        return () -> {
            currentOperation.compareAndSet(operation, null);
            if (operation.isTimedOut()) {
                throw operation.getTimeoutException();
            }
        };
    }

    @Override
    public void close() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
    }

    private boolean isEnabled() {
        return !timeout.isZero() && !timeout.isNegative();
    }

    private void checkCurrentOperation() {
        WatchedOperation operation = currentOperation.get();
        if (operation != null && operation.hasExpired(System.nanoTime())) {
            EmbeddedPythonOperationTimeoutException timeoutException =
                    operation.getTimeoutException();
            log.error(timeoutException.getMessage(), timeoutException);
            try {
                timeoutHandler.accept(timeoutException);
            } catch (Throwable t) {
                log.error("Failed to report embedded Python operation timeout.", t);
            }
        }
    }

    private static final class WatchedOperation {

        private final EmbeddedPythonOperationTimeoutException timeoutException;
        private final long timeoutNanos;
        private final long startNanos;
        private final AtomicBoolean timedOut = new AtomicBoolean(false);

        private WatchedOperation(
                String operationName, Duration timeout, long timeoutNanos, long startNanos) {
            this.timeoutException =
                    new EmbeddedPythonOperationTimeoutException(operationName, timeout);
            this.timeoutNanos = timeoutNanos;
            this.startNanos = startNanos;
        }

        private boolean hasExpired(long nowNanos) {
            return nowNanos - startNanos >= timeoutNanos && timedOut.compareAndSet(false, true);
        }

        private boolean isTimedOut() {
            return timedOut.get();
        }

        private EmbeddedPythonOperationTimeoutException getTimeoutException() {
            return timeoutException;
        }
    }
}
