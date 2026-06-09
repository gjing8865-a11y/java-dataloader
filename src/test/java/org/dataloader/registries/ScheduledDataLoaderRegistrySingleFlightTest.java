package org.dataloader.registries;

import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.dataloader.fixtures.parameterized.TestDataLoaderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.TWO_SECONDS;
import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.dataloader.fixtures.TestKit.snooze;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduledDataLoaderRegistrySingleFlightTest {

    private final BatchLoader<Object, Object> identityBatchLoader = CompletableFuture::completedFuture;

    @Test
    public void reschedule_now_keeps_a_single_pending_task_per_key() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        DataLoader<Object, Object> dataLoader = newDataLoader(identityBatchLoader);
        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dataLoader)
                .dispatchPredicate((key, dl) -> false)
                .scheduledExecutorService(executor)
                .schedule(Duration.ofDays(1))
                .build();

        for (int i = 0; i < 10; i++) {
            registry.rescheduleNow();
        }

        assertThat(executor.getQueue().size(), equalTo(1));

        registry.close();
        executor.shutdownNow();
    }

    @Test
    public void dispatch_all_keeps_a_single_pending_task_when_predicate_is_false_and_combine_preserves_config() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        DispatchPredicate predicate = (key, dl) -> false;
        DataLoader<Object, Object> dataLoader = newDataLoader(identityBatchLoader);
        ScheduledDataLoaderRegistry baseRegistry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dataLoader)
                .dispatchPredicate(predicate)
                .scheduledExecutorService(executor)
                .schedule(Duration.ofDays(1))
                .tickerMode(true)
                .build();

        ScheduledDataLoaderRegistry registry = baseRegistry.combine(DataLoaderRegistry.newRegistry().build());

        assertThat(registry.getScheduleDuration(), equalTo(Duration.ofDays(1)));
        assertThat(registry.isTickerMode(), equalTo(true));
        assertThat(registry.getDispatchPredicate(), equalTo(predicate));

        for (int i = 0; i < 10; i++) {
            registry.dispatchAll();
        }

        assertThat(executor.getQueue().size(), equalTo(1));

        registry.close();
        baseRegistry.close();
        executor.shutdownNow();
    }

    @Test
    public void close_cancels_pending_tasks_stops_predicate_growth_and_only_shuts_down_default_executor() {
        AtomicInteger defaultCounter = new AtomicInteger();
        DataLoader<Object, Object> defaultLoader = newDataLoader(identityBatchLoader);
        ScheduledDataLoaderRegistry defaultRegistry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", defaultLoader)
                .dispatchPredicate((key, dl) -> {
                    defaultCounter.incrementAndGet();
                    return false;
                })
                .schedule(Duration.ofMillis(10))
                .build();

        defaultRegistry.rescheduleNow();
        await().atMost(TWO_SECONDS).until(() -> defaultCounter.get() > 0);

        defaultRegistry.close();
        int defaultCountAfterClose = defaultCounter.get();
        snooze(150);
        assertThat(defaultCounter.get(), equalTo(defaultCountAfterClose));
        assertTrue(defaultRegistry.getScheduledExecutorService().isShutdown());

        ScheduledThreadPoolExecutor externalExecutor = new ScheduledThreadPoolExecutor(1);
        externalExecutor.setRemoveOnCancelPolicy(true);
        AtomicInteger externalCounter = new AtomicInteger();
        DataLoader<Object, Object> externalLoader = newDataLoader(identityBatchLoader);
        ScheduledDataLoaderRegistry externalRegistry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("b", externalLoader)
                .dispatchPredicate((key, dl) -> {
                    externalCounter.incrementAndGet();
                    return false;
                })
                .scheduledExecutorService(externalExecutor)
                .schedule(Duration.ofMillis(10))
                .build();

        externalRegistry.rescheduleNow();
        await().atMost(TWO_SECONDS).until(() -> externalCounter.get() > 0);

        externalRegistry.close();
        int externalCountAfterClose = externalCounter.get();
        snooze(150);
        assertThat(externalCounter.get(), equalTo(externalCountAfterClose));
        assertFalse(externalExecutor.isShutdown());

        externalExecutor.shutdownNow();
    }

    @Test
    public void unregister_cancels_pending_tasks_and_stops_testing_the_removed_key() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        AtomicInteger predicateCounter = new AtomicInteger();
        DispatchPredicate predicate = (key, dl) -> {
            predicateCounter.incrementAndGet();
            return false;
        };

        DataLoader<Object, Object> dataLoader = newDataLoader(identityBatchLoader);
        dataLoader.load("A");

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dataLoader, predicate)
                .scheduledExecutorService(executor)
                .schedule(Duration.ofSeconds(1))
                .build();

        registry.dispatchAll();

        assertThat(predicateCounter.get(), equalTo(1));
        assertThat(executor.getQueue().size(), equalTo(1));

        registry.unregister("a");

        await().atMost(TWO_SECONDS).until(() -> executor.getQueue().isEmpty());
        assertThat(predicateCounter.get(), equalTo(1));
        assertTrue(registry.getDataLoaderPredicates().isEmpty());

        registry.close();
        executor.shutdownNow();
    }

    @ParameterizedTest
    @MethodSource("org.dataloader.fixtures.parameterized.TestDataLoaderFactories#get")
    public void ticker_mode_keeps_chained_data_loaders_completing_with_single_flight_scheduling(TestDataLoaderFactory factory) {
        DataLoader<String, String> dlA = factory.idLoaderDelayed(Duration.ofMillis(100));
        DataLoader<String, String> dlB = factory.idLoaderDelayed(Duration.ofMillis(200));

        CompletableFuture<String> chainedCF = dlA.load("AK1").thenCompose(dlB::load);

        AtomicBoolean done = new AtomicBoolean();
        chainedCF.whenComplete((value, throwable) -> done.set(true));

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .register("b", dlB)
                .dispatchPredicate((key, dl) -> true)
                .schedule(Duration.ofMillis(10))
                .tickerMode(true)
                .build();

        int count = registry.dispatchAllWithCount();
        assertThat(count, equalTo(1));

        await().atMost(TWO_SECONDS).untilAtomic(done, is(true));

        registry.close();
    }
}
