package org.dataloader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.dataloader.DataLoaderFactory.newMappedDataLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests for context-aware cache APIs:
 * - getIfPresent(key, context)
 * - getIfCompleted(key, context)
 * - clear(key, context)
 * - prime(key, context, value)
 * - prime(key, context, error)
 * - prime(key, context, future)
 */
public class DataLoaderContextCacheTest {

    // A CacheKey that combines input key + context into a compound key.
    // This mirrors the typical real-world usage and ensures keys with different
    // contexts do not collide in the future cache.
    private static class ContextualCacheKey<K> implements CacheKey<K> {
        @Override
        public Object getKey(K input) {
            return input;
        }

        @Override
        public Object getKeyWithContext(K input, Object context) {
            return "k=" + input + "|ctx=" + context;
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> DataLoaderOptions optionsWithContext() {
        return DataLoaderOptions.newOptions()
                .setCacheKeyFunction(new ContextualCacheKey<>())
                .build();
    }

    // ------------------------------------------------------------------
    // BatchLoader tests
    // ------------------------------------------------------------------

    @Test
    public void same_key_different_contexts_yield_different_futures_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) {
                values.add("v-" + k);
            }
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        CompletableFuture<String> fA = dl.load(1, "ctxA");
        CompletableFuture<String> fB = dl.load(1, "ctxB");

        assertThat("Different contexts should not share the same future", fA, not(sameInstance(fB)));

        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfPresent(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfPresent(1, "ctxC").isPresent(), equalTo(false));
        assertThat(dl.getIfPresent(1).isPresent(), equalTo(false)); // plain-key not registered

        dl.dispatch();

        assertThat(batchCount.get(), equalTo(1)); // single batch contained both 1, 1
        assertThat(fA.join(), equalTo("v-1"));
        assertThat(fB.join(), equalTo("v-1"));

        // After dispatch, getIfCompleted should reflect the (key, context) pair
        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxC").isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(1).isPresent(), equalTo(false));
    }

    @Test
    public void clear_with_context_only_affects_that_context_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) {
                values.add("v-" + k + "#" + batchCount.get());
            }
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        dl.load(1, "ctxA");
        dl.load(1, "ctxB");
        dl.dispatch();
        assertThat(batchCount.get(), equalTo(1));

        // ctxA should be cleared, ctxB should remain cached
        dl.clear(1, "ctxA");

        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(false));
        assertThat(dl.getIfPresent(1, "ctxB").isPresent(), equalTo(true));

        CompletableFuture<String> reloadedA = dl.load(1, "ctxA");
        CompletableFuture<String> stillB = dl.load(1, "ctxB");
        dl.dispatch();

        // batch count increased because ctxA was a real miss
        assertThat(batchCount.get(), equalTo(2));
        assertThat(reloadedA.join(), equalTo("v-1#2"));
        // ctxB was hit from cache, no batch re-entry
        assertThat(stillB.join(), equalTo("v-1#1"));
    }

    @Test
    public void prime_value_only_affects_given_context_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) {
                values.add("v-" + k);
            }
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        dl.prime(1, "ctxA", "primedA");

        // ctxA is primed -- no batch call on load
        CompletableFuture<String> fA = dl.load(1, "ctxA");
        // ctxB is NOT primed -- triggers a batch call
        CompletableFuture<String> fB = dl.load(1, "ctxB");

        assertThat(fA.isDone(), equalTo(true));
        assertThat(fA.join(), equalTo("primedA"));

        dl.dispatch();
        assertThat(batchCount.get(), equalTo(1));
        assertThat(fB.join(), equalTo("v-1"));
    }

    @Test
    public void prime_error_only_affects_given_context_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) {
                values.add("v-" + k);
            }
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        Exception primeErr = new RuntimeException("primed-error");
        dl.prime(1, "ctxA", primeErr);

        CompletableFuture<String> fA = dl.load(1, "ctxA");
        CompletableFuture<String> fB = dl.load(1, "ctxB");

        // ctxA: should return the primed error
        assertThat(fA.isCompletedExceptionally(), equalTo(true));
        try {
            fA.join();
            assert false : "Expected exception";
        } catch (Throwable t) {
            assertThat(t.getCause(), instanceOf(RuntimeException.class));
            assertThat(t.getCause().getMessage(), equalTo("primed-error"));
        }

        dl.dispatch();
        assertThat(batchCount.get(), equalTo(1));
        assertThat(fB.join(), equalTo("v-1"));
    }

    @Test
    public void getIfCompleted_distinct_per_context_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) {
                values.add("v-" + k);
            }
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        dl.load(1, "ctxA");
        dl.load(1, "ctxB");

        // before dispatch: not completed, but present
        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfPresent(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(1, "ctxB").isPresent(), equalTo(false));

        dl.dispatch();
        assertThat(batchCount.get(), equalTo(1));

        // after dispatch: completed for both contexts, empty for unknown
        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxC").isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(2, "ctxA").isPresent(), equalTo(false));
    }

    @Test
    public void caching_disabled_queries_always_empty_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) {
                values.add("v-" + k);
            }
            return CompletableFuture.completedFuture(values);
        };

        DataLoaderOptions options = DataLoaderOptions.newOptions()
                .setCachingEnabled(false)
                .build();

        DataLoader<Integer, String> dl = newDataLoader(loader, options);

        CompletableFuture<String> f = dl.load(1, "ctxA");
        dl.dispatch();

        assertThat(batchCount.get(), equalTo(1));
        assertThat(f.join(), equalTo("v-1"));

        // getIfPresent / getIfCompleted for all queries are empty when caching is disabled
        assertThat(dl.getIfPresent(1).isPresent(), equalTo(false));
        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(1).isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(false));

        // priming while caching disabled: should not surface to getIfPresent
        dl.prime(1, "ctxA", "primed");
        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(false));
    }

    // ------------------------------------------------------------------
    // MappedBatchLoader tests
    // ------------------------------------------------------------------

    @Test
    public void same_key_different_contexts_yield_different_futures_mappedBatchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        MappedBatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            Map<Integer, String> map = new LinkedHashMap<>();
            for (Integer k : keys) {
                map.put(k, "v-" + k);
            }
            return CompletableFuture.completedFuture(map);
        };

        DataLoader<Integer, String> dl = newMappedDataLoader(loader, optionsWithContext());

        CompletableFuture<String> fA = dl.load(1, "ctxA");
        CompletableFuture<String> fB = dl.load(1, "ctxB");

        assertThat(fA, not(sameInstance(fB)));
        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfPresent(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfPresent(1).isPresent(), equalTo(false));

        dl.dispatch();

        assertThat(batchCount.get(), equalTo(1));
        assertThat(fA.join(), equalTo("v-1"));
        assertThat(fB.join(), equalTo("v-1"));

        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1).isPresent(), equalTo(false));
    }

    @Test
    public void clear_with_context_only_affects_that_context_mappedBatchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        MappedBatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            Map<Integer, String> map = new LinkedHashMap<>();
            for (Integer k : keys) {
                map.put(k, "v-" + k + "#" + batchCount.get());
            }
            return CompletableFuture.completedFuture(map);
        };

        DataLoader<Integer, String> dl = newMappedDataLoader(loader, optionsWithContext());

        dl.load(1, "ctxA");
        dl.load(1, "ctxB");
        dl.dispatch();
        assertThat(batchCount.get(), equalTo(1));

        dl.clear(1, "ctxA");

        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(false));
        assertThat(dl.getIfPresent(1, "ctxB").isPresent(), equalTo(true));

        CompletableFuture<String> reA = dl.load(1, "ctxA");
        CompletableFuture<String> reB = dl.load(1, "ctxB");
        dl.dispatch();

        assertThat(batchCount.get(), equalTo(2));
        assertThat(reA.join(), equalTo("v-1#2"));
        assertThat(reB.join(), equalTo("v-1#1"));
    }

    @Test
    public void prime_value_and_error_only_affect_given_context_mappedBatchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        MappedBatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            Map<Integer, String> map = new LinkedHashMap<>();
            for (Integer k : keys) {
                map.put(k, "v-" + k);
            }
            return CompletableFuture.completedFuture(map);
        };

        DataLoader<Integer, String> dl = newMappedDataLoader(loader, optionsWithContext());

        dl.prime(1, "ctxA", "primedA");
        dl.prime(1, "ctxB", new RuntimeException("primed-error-B"));

        CompletableFuture<String> fA = dl.load(1, "ctxA");
        CompletableFuture<String> fB = dl.load(1, "ctxB");
        CompletableFuture<String> fC = dl.load(1, "ctxC");

        assertThat(fA.join(), equalTo("primedA"));
        try {
            fB.join();
            assert false : "expected exception";
        } catch (Throwable t) {
            assertThat(t.getCause().getMessage(), equalTo("primed-error-B"));
        }

        dl.dispatch();
        assertThat(batchCount.get(), equalTo(1));
        assertThat(fC.join(), equalTo("v-1"));
    }

    @Test
    public void getIfCompleted_distinct_per_context_mappedBatchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        MappedBatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            Map<Integer, String> map = new LinkedHashMap<>();
            for (Integer k : keys) {
                map.put(k, "v-" + k);
            }
            return CompletableFuture.completedFuture(map);
        };

        DataLoader<Integer, String> dl = newMappedDataLoader(loader, optionsWithContext());

        dl.load(1, "ctxA");
        dl.load(1, "ctxB");

        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfPresent(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(1, "ctxB").isPresent(), equalTo(false));

        dl.dispatch();
        assertThat(batchCount.get(), equalTo(1));

        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxB").isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1, "ctxC").isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(2, "ctxA").isPresent(), equalTo(false));
    }

    @Test
    public void caching_disabled_queries_always_empty_mappedBatchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        MappedBatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            Map<Integer, String> map = new LinkedHashMap<>();
            for (Integer k : keys) {
                map.put(k, "v-" + k);
            }
            return CompletableFuture.completedFuture(map);
        };

        DataLoaderOptions options = DataLoaderOptions.newOptions()
                .setCachingEnabled(false)
                .build();

        DataLoader<Integer, String> dl = newMappedDataLoader(loader, options);

        CompletableFuture<String> f = dl.load(1, "ctxA");
        dl.dispatch();

        assertThat(batchCount.get(), equalTo(1));
        assertThat(f.join(), equalTo("v-1"));

        assertThat(dl.getIfPresent(1).isPresent(), equalTo(false));
        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(1).isPresent(), equalTo(false));
        assertThat(dl.getIfCompleted(1, "ctxA").isPresent(), equalTo(false));

        dl.prime(1, "ctxA", "primed");
        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(false));
    }

    // ------------------------------------------------------------------
    // Edge: prime(key, ctx, future) serves the provided future
    // ------------------------------------------------------------------
    @Test
    public void prime_future_is_served_to_load_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) {
                values.add("v-" + k);
            }
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        CompletableFuture<String> preComputed = CompletableFuture.completedFuture("pre-computed");
        dl.prime(1, "ctxA", preComputed);

        CompletableFuture<String> fA = dl.load(1, "ctxA");
        CompletableFuture<String> fB = dl.load(1, "ctxB");
        dl.dispatch();

        assertThat(fA, sameInstance(preComputed));
        assertThat(fA.join(), equalTo("pre-computed"));
        assertThat(batchCount.get(), equalTo(1)); // ctxB triggers a batch
        assertThat(fB.join(), equalTo("v-1"));
    }

    // ------------------------------------------------------------------
    // Edge: clear handler version works with context
    // ------------------------------------------------------------------
    @Test
    public void clear_with_handler_invokes_handler_batchLoader() {
        BatchLoader<Integer, String> loader = keys -> {
            List<String> values = new ArrayList<>();
            for (Integer k : keys) values.add("v-" + k);
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        dl.load(1, "ctxA");
        dl.dispatch();

        Optional<CompletableFuture<String>> before = dl.getIfPresent(1, "ctxA");
        assertThat(before.isPresent(), equalTo(true));

        AtomicInteger handlerCalls = new AtomicInteger(0);
        dl.clear(1, "ctxA", (v, t) -> handlerCalls.incrementAndGet());

        assertThat(dl.getIfPresent(1, "ctxA").isPresent(), equalTo(false));
        // handler should be invoked asynchronously by valueCache.delete().whenComplete()
        assertThat(handlerCalls.get() >= 1, equalTo(true));
    }

    // ------------------------------------------------------------------
    // Compatibility: old no-context APIs still behave as before
    // ------------------------------------------------------------------
    @Test
    public void old_api_unaffected_by_contextual_cache_key() {
        AtomicInteger batchCount = new AtomicInteger(0);
        BatchLoader<Integer, String> loader = keys -> {
            batchCount.incrementAndGet();
            List<String> values = new ArrayList<>();
            for (Integer k : keys) values.add("v-" + k);
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        CompletableFuture<String> f = dl.load(1); // no context
        dl.dispatch();

        assertThat(batchCount.get(), equalTo(1));
        assertThat(f.join(), equalTo("v-1"));

        // The old no-context API should still find the key previously loaded via load(k)
        assertThat(dl.getIfPresent(1).isPresent(), equalTo(true));
        assertThat(dl.getIfCompleted(1).isPresent(), equalTo(true));

        // clear by key only clears the no-context cache entry
        dl.clear(1);
        assertThat(dl.getIfPresent(1).isPresent(), equalTo(false));
    }

    // ------------------------------------------------------------------
    // Edge: load with distinct contexts still passes the key contexts through
    // to the batch environment
    // ------------------------------------------------------------------
    @Test
    public void load_with_distinct_contexts_passes_contexts_to_batch_environment_batchLoader() {
        AtomicInteger batchCount = new AtomicInteger(0);
        AtomicInteger seenContexts = new AtomicInteger(0);
        BatchLoaderWithContext<Integer, String> loader = (keys, env) -> {
            batchCount.incrementAndGet();
            List<Object> ctxList = env.getKeyContextsList();
            for (Object c : ctxList) {
                if (c != null) {
                    seenContexts.incrementAndGet();
                }
            }
            List<String> values = new ArrayList<>();
            for (Integer k : keys) values.add("v-" + k);
            return CompletableFuture.completedFuture(values);
        };

        DataLoader<Integer, String> dl = newDataLoader(loader, optionsWithContext());

        dl.load(1, "ctxA");
        dl.load(1, "ctxB");
        dl.load(2, "ctxA");
        dl.dispatch();

        assertThat(batchCount.get(), equalTo(1));
        // key 1 had 2 distinct context loads, key 2 had 1 context load
        assertThat(seenContexts.get(), equalTo(3));
    }
}
