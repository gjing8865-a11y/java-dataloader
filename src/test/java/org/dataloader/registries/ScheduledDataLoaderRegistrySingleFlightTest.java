package org.dataloader.registries;

import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.dataloader.fixtures.TestKit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.TWO_SECONDS;
import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ScheduledDataLoaderRegistrySingleFlightTest {

    final BatchLoader<Object, Object> identityBatchLoader = CompletableFuture::completedFuture;

    static class CountingDispatchPredicate implements DispatchPredicate {
        final AtomicInteger count = new AtomicInteger();
        final int max;

        public CountingDispatchPredicate(int max) {
            this.max = max;
        }

        @Override
        public boolean test(String dataLoaderKey, DataLoader<?, ?> dataLoader) {
            int current = count.incrementAndGet();
            return current > max;
        }
    }

    @Test
    public void rescheduleNow_10_times_only_1_task_in_executor_queue() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        DispatchPredicate neverDispatch = (key, dl) -> false;

        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        dlA.load("K1");

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate(neverDispatch)
                .scheduledExecutorService(executor)
                .schedule(Duration.ofHours(1))
                .build();

        for (int i = 0; i < 10; i++) {
            registry.rescheduleNow();
        }

        assertThat(executor.getQueue().size(), equalTo(1));

        registry.close();
        executor.shutdown();
    }

    @Test
    public void dispatchAll_10_times_with_predicate_false_only_1_pending_task() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        DispatchPredicate neverDispatch = (key, dl) -> false;

        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        dlA.load("K1");

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate(neverDispatch)
                .scheduledExecutorService(executor)
                .schedule(Duration.ofHours(1))
                .build();

        for (int i = 0; i < 10; i++) {
            registry.dispatchAll();
        }

        assertThat(executor.getQueue().size(), equalTo(1));

        registry.close();
        executor.shutdown();
    }

    @Test
    public void close_stops_predicate_count_from_growing() {
        AtomicInteger predicateCount = new AtomicInteger();
        DispatchPredicate countingFalse = (key, dl) -> {
            predicateCount.incrementAndGet();
            return false;
        };

        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        dlA.load("K1");

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate(countingFalse)
                .schedule(Duration.ofMillis(10))
                .build();

        registry.rescheduleNow();

        TestKit.snooze(200);
        assertThat(predicateCount.get() > 0, equalTo(true));

        registry.close();

        int countAfterClose = predicateCount.get();
        TestKit.snooze(200);
        assertThat(predicateCount.get(), equalTo(countAfterClose));
    }

    @Test
    public void unregister_stops_predicate_testing_for_that_key() {
        AtomicInteger predicateACount = new AtomicInteger();
        AtomicInteger predicateBCount = new AtomicInteger();

        DispatchPredicate predicateA = (key, dl) -> {
            predicateACount.incrementAndGet();
            return false;
        };
        DispatchPredicate predicateB = (key, dl) -> {
            predicateBCount.incrementAndGet();
            return false;
        };

        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        DataLoader<Object, Object> dlB = newDataLoader(identityBatchLoader);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA, predicateA)
                .register("b", dlB, predicateB)
                .schedule(Duration.ofMillis(10))
                .build();

        registry.rescheduleNow();

        TestKit.snooze(200);
        assertThat(predicateACount.get() > 0, equalTo(true));
        assertThat(predicateBCount.get() > 0, equalTo(true));

        registry.unregister("a");

        int countA = predicateACount.get();
        int countB = predicateBCount.get();

        TestKit.snooze(200);

        assertThat(predicateACount.get(), equalTo(countA));
        assertThat(predicateBCount.get() > countB, equalTo(true));

        registry.close();
    }

    @Test
    public void tickerMode_chained_data_loader_still_completes() {
        DataLoader<String, String> dlA = newDataLoader(keys -> CompletableFuture.supplyAsync(() -> {
            TestKit.snooze(100);
            return keys;
        }));
        DataLoader<String, String> dlB = newDataLoader(keys -> CompletableFuture.supplyAsync(() -> {
            TestKit.snooze(100);
            return keys;
        }));

        CompletableFuture<String> chainedCF = dlA.load("AK1").thenCompose(dlB::load);

        AtomicBoolean done = new AtomicBoolean();
        chainedCF.whenComplete((v, t) -> done.set(true));

        DispatchPredicate alwaysDispatch = (key, dl) -> true;

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .register("b", dlB)
                .dispatchPredicate(alwaysDispatch)
                .schedule(Duration.ofMillis(10))
                .tickerMode(true)
                .build();

        assertThat(registry.isTickerMode(), equalTo(true));

        int count = registry.dispatchAllWithCount();
        assertThat(count, equalTo(1));

        await().atMost(TWO_SECONDS).untilAtomic(done, is(true));

        registry.close();
    }

    @Test
    public void combine_preserves_schedule_tickerMode_and_predicate() {
        DispatchPredicate customPredicate = (key, dl) -> true;

        ScheduledDataLoaderRegistry registry1 = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .dispatchPredicate(customPredicate)
                .schedule(Duration.ofSeconds(5))
                .tickerMode(true)
                .register("a", newDataLoader(identityBatchLoader))
                .build();

        DataLoaderRegistry registry2 = DataLoaderRegistry.newRegistry()
                .register("b", newDataLoader(identityBatchLoader))
                .build();

        ScheduledDataLoaderRegistry combined = registry1.combine(registry2);

        assertThat(combined.getScheduleDuration(), equalTo(Duration.ofSeconds(5)));
        assertThat(combined.isTickerMode(), equalTo(true));
        assertThat(combined.getDispatchPredicate(), equalTo(customPredicate));
        assertThat(combined.getDataLoadersMap().containsKey("a"), equalTo(true));
        assertThat(combined.getDataLoadersMap().containsKey("b"), equalTo(true));

        combined.close();
    }

    @Test
    public void multiple_keys_each_get_only_1_pending_task() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        DispatchPredicate neverDispatch = (key, dl) -> false;

        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        DataLoader<Object, Object> dlB = newDataLoader(identityBatchLoader);
        DataLoader<Object, Object> dlC = newDataLoader(identityBatchLoader);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .register("b", dlB)
                .register("c", dlC)
                .dispatchPredicate(neverDispatch)
                .scheduledExecutorService(executor)
                .schedule(Duration.ofHours(1))
                .build();

        for (int i = 0; i < 10; i++) {
            registry.dispatchAll();
        }

        assertThat(executor.getQueue().size(), equalTo(3));

        registry.close();
        executor.shutdown();
    }
}