package org.dataloader.registries;

import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.FIVE_SECONDS;
import static org.dataloader.fixtures.TestKit.snooze;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduledDataLoaderRegistrySingleFlightTest {

    final BatchLoader<Object, Object> identityBatchLoader = CompletableFuture::completedFuture;

    @Test
    public void repeated_rescheduleNow_only_one_pending_per_key() {
        DataLoader<Object, Object> dlA = DataLoaderFactory.newDataLoader(identityBatchLoader);

        DispatchPredicate neverDispatch = (key, dl) -> false;

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate(neverDispatch)
                .schedule(Duration.ofSeconds(10))
                .build();

        for (int i = 0; i < 10; i++) {
            registry.rescheduleNow();
        }

        assertThat(registry.getPendingCount(), equalTo(1));
        assertTrue(registry.hasPending("a"));

        registry.close();
    }

    @Test
    public void repeated_dispatchAll_predicate_false_only_one_pending_per_key() {
        DataLoader<Object, Object> dlA = DataLoaderFactory.newDataLoader(identityBatchLoader);
        DataLoader<Object, Object> dlB = DataLoaderFactory.newDataLoader(identityBatchLoader);

        DispatchPredicate neverDispatch = (key, dl) -> false;

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .register("b", dlB)
                .dispatchPredicate(neverDispatch)
                .schedule(Duration.ofSeconds(10))
                .build();

        for (int i = 0; i < 10; i++) {
            registry.dispatchAll();
        }

        assertThat(registry.getPendingCount(), equalTo(2));
        assertTrue(registry.hasPending("a"));
        assertTrue(registry.hasPending("b"));

        registry.close();
    }

    @Test
    public void close_stops_predicate_counting() {
        AtomicInteger counter = new AtomicInteger();
        DispatchPredicate countingPredicate = (key, dl) -> {
            counter.incrementAndGet();
            return false;
        };

        DataLoader<Object, Object> dlA = DataLoaderFactory.newDataLoader(identityBatchLoader);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate(countingPredicate)
                .schedule(Duration.ofMillis(10))
                .build();

        registry.rescheduleNow();

        snooze(200);
        assertTrue(counter.get() > 0);

        registry.close();

        int countThen = counter.get();

        snooze(200);
        assertEquals(counter.get(), countThen);

        registry.dispatchAll();
        snooze(200);
        assertEquals(counter.get(), countThen);

        registry.rescheduleNow();
        snooze(200);
        assertEquals(counter.get(), countThen);
    }

    @Test
    public void unregister_stops_predicate_for_key() {
        AtomicInteger counterA = new AtomicInteger();
        AtomicInteger counterB = new AtomicInteger();

        DispatchPredicate predicateA = (key, dl) -> {
            counterA.incrementAndGet();
            return false;
        };
        DispatchPredicate predicateB = (key, dl) -> {
            counterB.incrementAndGet();
            return false;
        };

        DataLoader<Object, Object> dlA = DataLoaderFactory.newDataLoader(identityBatchLoader);
        DataLoader<Object, Object> dlB = DataLoaderFactory.newDataLoader(identityBatchLoader);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA, predicateA)
                .register("b", dlB, predicateB)
                .schedule(Duration.ofMillis(10))
                .build();

        registry.rescheduleNow();

        snooze(200);
        assertTrue(counterA.get() > 0);
        assertTrue(counterB.get() > 0);

        int countAThen = counterA.get();
        int countBThen = counterB.get();

        registry.unregister("a");

        assertFalse(registry.hasPending("a"));
        assertTrue(registry.hasPending("b"));

        snooze(200);

        assertEquals(counterA.get(), countAThen);
        assertTrue(counterB.get() > countBThen);

        registry.close();
    }

    @Test
    public void tickerMode_chained_data_loaders_still_complete() {
        List<Collection<String>> aCalls = new ArrayList<>();
        List<Collection<String>> bCalls = new ArrayList<>();

        BatchLoader<String, String> batchLoaderA = keys -> {
            aCalls.add(new ArrayList<>(keys));
            CompletableFuture<List<String>> result = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> {
                snooze(50);
                result.complete(new ArrayList<>(keys));
            });
            return result;
        };

        BatchLoader<String, String> batchLoaderB = keys -> {
            bCalls.add(new ArrayList<>(keys));
            CompletableFuture<List<String>> result = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> {
                snooze(50);
                result.complete(new ArrayList<>(keys));
            });
            return result;
        };

        DataLoader<String, String> dlA = DataLoaderFactory.newDataLoader(batchLoaderA);
        DataLoader<String, String> dlB = DataLoaderFactory.newDataLoader(batchLoaderB);

        CompletableFuture<String> chainedCF = dlA.load("A").thenCompose(dlB::load);

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

        registry.dispatchAll();

        await().atMost(FIVE_SECONDS).untilAtomic(done, is(true));

        assertThat(chainedCF.join(), equalTo("A"));

        registry.close();
    }
}
