package org.dataloader;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class DataLoaderContextCacheTest {

    private static class Fixture {
        AtomicInteger batchCallCount = new AtomicInteger();
        DataLoader<String, String> dataLoader;

        Fixture(boolean useMapped, boolean cachingEnabled) {
            CacheKey<String> cacheKeyFn = new CacheKey<String>() {
                @Override
                public Object getKey(String input) {
                    return input;
                }
                @Override
                public Object getKeyWithContext(String input, Object context) {
                    return input + "-" + context;
                }
            };
            DataLoaderOptions options = DataLoaderOptions.newOptions()
                    .setCachingEnabled(cachingEnabled)
                    .setCacheKeyFunction(cacheKeyFn)
                    .build();

            if (useMapped) {
                MappedBatchLoaderWithContext<String, String> mappedBatchLoader = (keys, env) -> {
                    batchCallCount.incrementAndGet();
                    Map<String, String> result = new HashMap<>();
                    for (String key : keys) {
                        Object ctx = env.getKeyContexts().get(key);
                        result.put(key, key + "-" + ctx);
                    }
                    return CompletableFuture.completedFuture(result);
                };
                dataLoader = DataLoaderFactory.newMappedDataLoader(mappedBatchLoader, options);
            } else {
                BatchLoaderWithContext<String, String> batchLoader = (keys, env) -> {
                    batchCallCount.incrementAndGet();
                    List<Object> contexts = env.getKeyContextsList();
                    List<String> result = new java.util.ArrayList<>();
                    for (int i = 0; i < keys.size(); i++) {
                        result.add(keys.get(i) + "-" + contexts.get(i));
                    }
                    return CompletableFuture.completedFuture(result);
                };
                dataLoader = DataLoaderFactory.newDataLoader(batchLoader, options);
            }
        }
    }

    private void runTests(boolean cachingEnabled, java.util.function.BiConsumer<Fixture, Boolean> test) {
        test.accept(new Fixture(false, cachingEnabled), false);
        test.accept(new Fixture(true, cachingEnabled), true);
    }

    @Test
    public void testSameKeyDifferentContextDoesNotShareFuture() {
        runTests(true, (fixture, useMapped) -> {
            DataLoader<String, String> dl = fixture.dataLoader;
            CompletableFuture<String> f1 = dl.load("A", "ctx1");
            CompletableFuture<String> f2 = dl.load("A", "ctx2");
            CompletableFuture<String> f3 = dl.load("A", "ctx1");

            dl.dispatchAndJoin();

            if (!useMapped) {
                assertEquals("A-ctx1", f1.join());
                assertEquals("A-ctx2", f2.join());
            }
            assertSame(f1, f3); // same key and context shares future
            assertNotSame(f1, f2); // same key different context does not share future

            assertEquals(1, fixture.batchCallCount.get()); // dispatched once
        });
    }

    @Test
    public void testClearOnlyClearsSpecifiedContext() {
        runTests(true, (fixture, useMapped) -> {
            DataLoader<String, String> dl = fixture.dataLoader;
            dl.load("A", "ctx1");
            dl.load("A", "ctx2");
            dl.dispatchAndJoin();

            assertEquals(1, fixture.batchCallCount.get());

            dl.clear("A", "ctx1");

            dl.load("A", "ctx1");
            dl.load("A", "ctx2");
            dl.dispatchAndJoin();

            assertEquals(2, fixture.batchCallCount.get()); // second dispatch for ctx1 only
        });
    }

    @Test
    public void testPrimeValueOnlyAffectsSpecifiedContext() {
        runTests(true, (fixture, useMapped) -> {
            DataLoader<String, String> dl = fixture.dataLoader;
            dl.prime("A", "ctx1", "PRIMED_A_CTX1");

            CompletableFuture<String> f1 = dl.load("A", "ctx1");
            CompletableFuture<String> f2 = dl.load("A", "ctx2");

            dl.dispatchAndJoin();

            assertEquals("PRIMED_A_CTX1", f1.join());
            if (!useMapped) {
                assertEquals("A-ctx2", f2.join());
            }
            assertEquals(1, fixture.batchCallCount.get()); // dispatched once for ctx2
        });
    }

    @Test
    public void testPrimeExceptionOnlyAffectsSpecifiedContext() {
        runTests(true, (fixture, useMapped) -> {
            DataLoader<String, String> dl = fixture.dataLoader;
            Exception ex = new RuntimeException("ERROR");
            dl.prime("A", "ctx1", ex);

            CompletableFuture<String> f1 = dl.load("A", "ctx1");
            CompletableFuture<String> f2 = dl.load("A", "ctx2");

            dl.dispatchAndJoin();

            assertTrue(f1.isCompletedExceptionally());
            if (!useMapped) {
                assertEquals("A-ctx2", f2.join());
            }
            assertEquals(1, fixture.batchCallCount.get()); // dispatched once for ctx2
        });
    }

    @Test
    public void testGetIfCompletedBehavior() {
        runTests(true, (fixture, useMapped) -> {
            DataLoader<String, String> dl = fixture.dataLoader;

            assertFalse(dl.getIfPresent("A", "ctx1").isPresent());
            assertFalse(dl.getIfCompleted("A", "ctx1").isPresent());

            dl.load("A", "ctx1");

            assertTrue(dl.getIfPresent("A", "ctx1").isPresent());
            assertFalse(dl.getIfCompleted("A", "ctx1").isPresent()); // not completed yet

            dl.dispatchAndJoin();

            assertTrue(dl.getIfPresent("A", "ctx1").isPresent());
            assertTrue(dl.getIfCompleted("A", "ctx1").isPresent());
            if (!useMapped) {
                assertEquals("A-ctx1", dl.getIfCompleted("A", "ctx1").get().join());
            }

            // context 2 is empty
            assertFalse(dl.getIfPresent("A", "ctx2").isPresent());
            assertFalse(dl.getIfCompleted("A", "ctx2").isPresent());
        });
    }

    @Test
    public void testCachingDisabledAlwaysEmpty() {
        runTests(false, (fixture, useMapped) -> {
            DataLoader<String, String> dl = fixture.dataLoader;

            dl.prime("A", "ctx1", "PRIMED");
            assertFalse(dl.getIfPresent("A", "ctx1").isPresent());
            assertFalse(dl.getIfCompleted("A", "ctx1").isPresent());

            dl.load("A", "ctx1");
            assertFalse(dl.getIfPresent("A", "ctx1").isPresent());
            assertFalse(dl.getIfCompleted("A", "ctx1").isPresent());

            dl.dispatchAndJoin();

            assertFalse(dl.getIfPresent("A", "ctx1").isPresent());
            assertFalse(dl.getIfCompleted("A", "ctx1").isPresent());

            dl.load("A", "ctx1");
            dl.dispatchAndJoin();
            assertEquals(2, fixture.batchCallCount.get()); // dispatched twice since caching is disabled
        });
    }
}
