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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.object.PyIterator;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link EmbeddedPythonIterator}. */
class EmbeddedPythonIteratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedPythonIteratorTest.class);

    @Test
    void testReadsIteratorLoadedByDifferentClassLoader() throws Exception {
        URL classesUrl =
                EmbeddedPythonIteratorTest.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation();

        try (URLClassLoader classLoader = new URLClassLoader(new URL[] {classesUrl}, null)) {
            Class<?> iteratorClass =
                    Class.forName(
                            "org.apache.flink.streaming.api.operators.python.embedded."
                                    + "ForeignClassLoaderIterator",
                            true,
                            classLoader);
            Object iterator =
                    iteratorClass
                            .getConstructor(Object[].class)
                            .newInstance((Object) new Object[] {"first", "second"});

            assertThat(iterator.getClass().getClassLoader())
                    .isNotSameAs(EmbeddedPythonIteratorTest.class.getClassLoader());

            try (EmbeddedPythonIterator embeddedPythonIterator =
                    EmbeddedPythonIterator.from(iterator)) {
                assertThat(embeddedPythonIterator.hasNext()).isTrue();
                assertThat(embeddedPythonIterator.next()).isEqualTo("first");
                assertThat(embeddedPythonIterator.hasNext()).isTrue();
                assertThat(embeddedPythonIterator.next()).isEqualTo("second");
                assertThat(embeddedPythonIterator.hasNext()).isFalse();
            }

            assertThat(iteratorClass.getMethod("isClosed").invoke(iterator)).isEqualTo(true);
        }
    }

    @Test
    void testReproducesDirectPemjaCastFailureAcrossClassLoaders() throws Exception {
        Object iterator = createForeignPemjaIterator();

        assertThat(iterator.getClass().getName()).isEqualTo(PyIterator.class.getName());
        assertThat(iterator.getClass()).isNotEqualTo(PyIterator.class);
        assertThat(iterator.getClass().getClassLoader())
                .isNotSameAs(PyIterator.class.getClassLoader());

        assertThatThrownBy(() -> castToLocalPemjaIterator(iterator))
                .isInstanceOf(ClassCastException.class)
                .hasMessageContaining("pemja.core.object.PyIterator")
                .hasMessageContaining("cannot be cast");
    }

    @Test
    void testWrapsPemjaIteratorLoadedByDifferentClassLoaderWithoutCastFailure() throws Exception {
        Object iterator = createForeignPemjaIterator();

        assertThatCode(() -> EmbeddedPythonIterator.from(iterator)).doesNotThrowAnyException();
    }

    @Test
    void testFailsStalledIteratorAndClosesIt() throws Exception {
        BlockingIterator iterator = new BlockingIterator();
        CountDownLatch timeoutReported = new CountDownLatch(1);
        AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
        try (EmbeddedPythonOperationWatchdog watchdog =
                        new EmbeddedPythonOperationWatchdog(
                                Duration.ofMillis(50),
                                failure -> {
                                    reportedFailure.set(failure);
                                    timeoutReported.countDown();
                                    iterator.unblock();
                                },
                                LOG);
                ExecutorServiceResource executorResource = new ExecutorServiceResource()) {
            Future<Void> consumeIterator =
                    executorResource
                            .getExecutorService()
                            .submit(
                                    () -> {
                                        consumeIterator(watchdog, iterator);
                                        return null;
                                    });

            assertThat(iterator.awaitEnteredHasNext()).isTrue();
            assertThat(timeoutReported.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reportedFailure.get())
                    .isInstanceOf(EmbeddedPythonOperationTimeoutException.class);
            assertThatThrownBy(() -> consumeIterator.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(EmbeddedPythonOperationTimeoutException.class);
            assertThat(iterator.isClosed()).isTrue();
        }
    }

    @Test
    void testIteratorIsClosedWhenHasNextThrows() {
        ThrowingHasNextIterator iterator = new ThrowingHasNextIterator();
        try (EmbeddedPythonOperationWatchdog watchdog =
                new EmbeddedPythonOperationWatchdog(
                        Duration.ofMinutes(1), failure -> fail("Unexpected timeout"), LOG)) {
            assertThatThrownBy(() -> consumeIterator(watchdog, iterator))
                    .isInstanceOf(TestIteratorException.class);
        }
        assertThat(iterator.isClosed()).isTrue();
    }

    @Test
    void testIteratorIsClosedWhenNextThrows() {
        ThrowingNextIterator iterator = new ThrowingNextIterator();
        try (EmbeddedPythonOperationWatchdog watchdog =
                new EmbeddedPythonOperationWatchdog(
                        Duration.ofMinutes(1), failure -> fail("Unexpected timeout"), LOG)) {
            assertThatThrownBy(() -> consumeIterator(watchdog, iterator))
                    .isInstanceOf(TestIteratorException.class);
        }
        assertThat(iterator.isClosed()).isTrue();
    }

    private static Object createForeignPemjaIterator() throws Exception {
        URL pemjaJarUrl = PyIterator.class.getProtectionDomain().getCodeSource().getLocation();

        try (URLClassLoader classLoader = new URLClassLoader(new URL[] {pemjaJarUrl}, null)) {
            Class<?> iteratorClass =
                    Class.forName("pemja.core.object.PyIterator", true, classLoader);
            Constructor<?> constructor =
                    iteratorClass.getDeclaredConstructor(long.class, long.class);
            constructor.setAccessible(true);
            return constructor.newInstance(0L, 0L);
        }
    }

    private static PyIterator castToLocalPemjaIterator(Object iterator) {
        return (PyIterator) iterator;
    }

    private static void consumeIterator(EmbeddedPythonOperationWatchdog watchdog, Object iterator)
            throws Exception {
        try (AutoCloseable ignored = watchdog.watch("operation.process_element");
                EmbeddedPythonIterator results = EmbeddedPythonIterator.from(iterator)) {
            while (results.hasNext()) {
                results.next();
            }
        }
    }

    public static final class BlockingIterator {
        private final CountDownLatch enteredHasNext = new CountDownLatch(1);
        private final CountDownLatch unblockHasNext = new CountDownLatch(1);
        private volatile boolean closed;

        public boolean hasNext() throws InterruptedException {
            enteredHasNext.countDown();
            unblockHasNext.await();
            return false;
        }

        public Object next() {
            throw new AssertionError("next should not be called");
        }

        public void close() {
            closed = true;
        }

        boolean awaitEnteredHasNext() throws InterruptedException {
            return enteredHasNext.await(5, TimeUnit.SECONDS);
        }

        void unblock() {
            unblockHasNext.countDown();
        }

        boolean isClosed() {
            return closed;
        }
    }

    public static final class ThrowingHasNextIterator {
        private boolean closed;

        public boolean hasNext() throws TestIteratorException {
            throw new TestIteratorException();
        }

        public Object next() {
            throw new AssertionError("next should not be called");
        }

        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    public static final class ThrowingNextIterator {
        private boolean closed;

        public boolean hasNext() {
            return true;
        }

        public Object next() throws TestIteratorException {
            throw new TestIteratorException();
        }

        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    private static final class TestIteratorException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    private static final class ExecutorServiceResource implements AutoCloseable {
        private final ExecutorService executorService = Executors.newSingleThreadExecutor();

        private ExecutorService getExecutorService() {
            return executorService;
        }

        @Override
        public void close() {
            executorService.shutdownNow();
        }
    }
}
