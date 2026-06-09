package org.dataloader.registries;

import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduledDataLoaderRegistrySingleFlightTest {

    private BatchLoader<Object, Object> identityBatchLoader = keys -> CompletableFuture.completedFuture(keys);

    @Test
    public void testRescheduleNowSingleFlight() {
        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate((k, dl) -> false)
                .schedule(Duration.ofHours(1))
                .scheduledExecutorService(executor)
                .build();

        for (int i = 0; i < 10; i++) {
            registry.rescheduleNow();
        }

        assertEquals(1, executor.getQueue().size());
        registry.close();
    }

    @Test
    public void testDispatchAllSingleFlight() {
        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate((k, dl) -> false)
                .schedule(Duration.ofHours(1))
                .scheduledExecutorService(executor)
                .build();

        for (int i = 0; i < 10; i++) {
            registry.dispatchAll();
        }

        assertEquals(1, executor.getQueue().size());
        registry.close();
    }

    @Test
    public void testCloseStopsPredicate() throws InterruptedException {
        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        AtomicInteger predicateCount = new AtomicInteger(0);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate((k, dl) -> {
                    predicateCount.incrementAndGet();
                    return false;
                })
                .schedule(Duration.ofMillis(10))
                .build();

        registry.dispatchAll();
        
        await().until(() -> predicateCount.get() > 2);
        registry.close();

        int countAfterClose = predicateCount.get();
        Thread.sleep(100);
        assertEquals(countAfterClose, predicateCount.get());
    }

    @Test
    public void testUnregisterStopsPredicate() throws InterruptedException {
        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        AtomicInteger predicateCount = new AtomicInteger(0);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .dispatchPredicate((k, dl) -> {
                    predicateCount.incrementAndGet();
                    return false;
                })
                .schedule(Duration.ofMillis(10))
                .build();

        registry.dispatchAll();

        await().until(() -> predicateCount.get() > 2);
        registry.unregister("a");

        int countAfterUnregister = predicateCount.get();
        Thread.sleep(100);
        assertEquals(countAfterUnregister, predicateCount.get());
        registry.close();
    }

    @Test
    public void testTickerModeChainedDataLoader() {
        DataLoader<Object, Object> dlA = newDataLoader(identityBatchLoader);
        DataLoader<Object, Object> dlB = newDataLoader(identityBatchLoader);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("a", dlA)
                .register("b", dlB)
                .tickerMode(true)
                .schedule(Duration.ofMillis(10))
                .build();

        CompletableFuture<Object> chainedFuture = dlA.load("A")
                .thenCompose(val -> dlB.load(val));

        registry.dispatchAll();

        await().until(chainedFuture::isDone, is(true));
        assertEquals("A", chainedFuture.join());
        
        registry.close();
    }
}
