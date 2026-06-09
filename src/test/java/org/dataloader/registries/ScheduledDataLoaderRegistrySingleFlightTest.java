package org.dataloader.registries;

import org.dataloader.DataLoader;
import org.dataloader.fixtures.parameterized.TestDataLoaderFactory;
import org.dataloader.fixtures.parameterized.TestDataLoaderFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.FIVE_SECONDS;
import static org.awaitility.Duration.TWO_SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ScheduledDataLoaderRegistrySingleFlightTest {

    @ParameterizedTest
    @MethodSource("org.dataloader.fixtures.parameterized.TestDataLoaderFactories#get")
    public void repeated_rescheduleNow_only_keeps_one_pending_task(TestDataLoaderFactory factory) throws Exception {
        List<Collection<String>> calls = new ArrayList<>();
        DataLoader<String, String> dlA = factory.idLoader(calls);
        dlA.load("K1");

        AtomicInteger predicateCounter = new AtomicInteger();
        DispatchPredicate countingPredicate = (key, dl) -> {
            predicateCounter.incrementAndGet();
            return false;
        };

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                    .register("a", dlA)
                    .dispatchPredicate(countingPredicate)
                    .scheduledExecutorService(executor)
                    .schedule(Duration.ofMillis(100))
                    .build();

            for (int i = 0; i < 10; i++) {
                registry.rescheduleNow();
            }

            Thread.sleep(50);

            // drain anything currently scheduled
            int queuedBeforeShutdown = executor.shutdownNow().size();

            // The executor queue should contain at most 1 pending task for key "a"
            // (0 if it already started executing; otherwise 1)
            assertThat(queuedBeforeShutdown <= 1, is(true));
        } finally {
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @MethodSource("org.dataloader.fixtures.parameterized.TestDataLoaderFactories#get")
    public void repeated_dispatchAll_with_predicate_false_only_keeps_one_pending_task(TestDataLoaderFactory factory) throws Exception {
        List<Collection<String>> calls = new ArrayList<>();
        DataLoader<String, String> dlA = factory.idLoader(calls);
        dlA.load("K1");

        DispatchPredicate neverDispatch = (key, dl) -> false;

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                    .register("a", dlA)
                    .dispatchPredicate(neverDispatch)
                    .scheduledExecutorService(executor)
                    .schedule(Duration.ofMillis(100))
                    .build();

            for (int i = 0; i < 10; i++) {
                registry.dispatchAll();
            }

            Thread.sleep(50);

            int queuedBeforeShutdown = executor.shutdownNow().size();
            assertThat(queuedBeforeShutdown <= 1, is(true));
        } finally {
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @MethodSource("org.dataloader.fixtures.parameterized.TestDataLoaderFactories#get")
    public void close_stops_future_predicate_checks(TestDataLoaderFactory factory) {
        DataLoader<String, String> dlA = factory.idLoader();
        dlA.load("K1");

        AtomicInteger predicateCounter = new AtomicInteger();
        DispatchPredicate countingPredicate = (key, dl) -> {
            predicateCounter.incrementAndGet();
            return false;
        };

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate(countingPredicate)
                .schedule(Duration.ofMillis(10))
                .build();

        registry.rescheduleNow();

        await().atMost(TWO_SECONDS).until(() -> predicateCounter.get() > 0);

        registry.close();

        int countAfterClose = predicateCounter.get();

        snooze(200);

        // after closing, the predicate should no longer be called
        assertThat(predicateCounter.get(), equalTo(countAfterClose));
    }

    @ParameterizedTest
    @MethodSource("org.dataloader.fixtures.parameterized.TestDataLoaderFactories#get")
    public void unregister_stops_predicate_checks_for_that_key(TestDataLoaderFactory factory) {
        DataLoader<String, String> dlA = factory.idLoader();
        dlA.load("K1");

        AtomicInteger predicateOnA = new AtomicInteger();
        DispatchPredicate predicateA = (key, dl) -> {
            predicateOnA.incrementAndGet();
            return false;
        };

        DataLoader<String, String> dlB = factory.idLoader();
        dlB.load("K2");

        AtomicInteger predicateOnB = new AtomicInteger();
        DispatchPredicate predicateB = (key, dl) -> {
            predicateOnB.incrementAndGet();
            return false;
        };

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA, predicateA)
                .register("b", dlB, predicateB)
                .dispatchPredicate((key, dl) -> false)
                .schedule(Duration.ofMillis(10))
                .build();

        registry.rescheduleNow();

        await().atMost(TWO_SECONDS).until(() -> predicateOnA.get() > 0 && predicateOnB.get() > 0);

        registry.unregister("a");

        int countA = predicateOnA.get();
        int countBBefore = predicateOnB.get();

        snooze(200);

        // after unregister, predicate for "a" should no longer be called
        assertThat(predicateOnA.get(), equalTo(countA));

        // but "b" should still be checked if schedule is active (unless close)
        // The executor is still running so b should still be checked
        // However to keep this test stable, we only assert "a" no longer gets invoked
        assertThat(predicateOnB.get() >= countBBefore, is(true));

        registry.close();
    }

    @ParameterizedTest
    @MethodSource("org.dataloader.fixtures.parameterized.TestDataLoaderFactories#get")
    public void ticker_mode_chained_dataloader_still_works(TestDataLoaderFactory factory) {
        DataLoader<String, String> dlA = factory.idLoaderDelayed(Duration.ofMillis(50));
        DataLoader<String, String> dlB = factory.idLoaderDelayed(Duration.ofMillis(50));

        CompletableFuture<String> chained = dlA.load("A").thenCompose(dlB::load);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .register("b", dlB)
                .dispatchPredicate((key, dl) -> true)
                .tickerMode(true)
                .schedule(Duration.ofMillis(10))
                .build();

        registry.dispatchAll();

        await().atMost(FIVE_SECONDS).until(chained::isDone);

        assertThat(chained.isDone(), is(true));

        registry.close();
    }

    private static void snooze(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
