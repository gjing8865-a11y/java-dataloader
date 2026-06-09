package org.dataloader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DataLoaderContextCacheTest {

    @Test
    public void same_key_different_context_should_not_share_future_batch_loader() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        CompletableFuture<String> futureCtx2 = loader.load("A", "ctx2");
        CompletableFuture<String> futureNoCtx = loader.load("A");

        assertThat(futureCtx1, not(sameInstance(futureCtx2)));
        assertThat(futureCtx1, not(sameInstance(futureNoCtx)));
        assertThat(futureCtx2, not(sameInstance(futureNoCtx)));

        loader.dispatch();

        assertThat(futureCtx1.join(), equalTo("A"));
        assertThat(futureCtx2.join(), equalTo("A"));
        assertThat(futureNoCtx.join(), equalTo("A"));

        assertThat(loadCalls.size(), equalTo(1));
        assertThat(loadCalls.get(0), equalTo(asList("A", "A", "A")));
    }

    @Test
    public void same_key_different_context_should_not_share_future_mapped_batch_loader() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newMappedDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            Map<String, String> map = new java.util.HashMap<>();
            keys.forEach(k -> map.put(k, k));
            return completedFuture(map);
        });

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        CompletableFuture<String> futureCtx2 = loader.load("A", "ctx2");
        CompletableFuture<String> futureNoCtx = loader.load("A");

        assertThat(futureCtx1, not(sameInstance(futureCtx2)));
        assertThat(futureCtx1, not(sameInstance(futureNoCtx)));
        assertThat(futureCtx2, not(sameInstance(futureNoCtx)));

        loader.dispatch();

        assertThat(futureCtx1.join(), equalTo("A"));
        assertThat(futureCtx2.join(), equalTo("A"));
        assertThat(futureNoCtx.join(), equalTo("A"));

        assertThat(loadCalls.size(), equalTo(1));
        assertThat(loadCalls.get(0), equalTo(singletonList("A")));
    }

    @Test
    public void getIfPresent_with_context_should_return_correct_future() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        Optional<CompletableFuture<String>> present1 = loader.getIfPresent("A", "ctx1");
        assertThat(present1.isPresent(), is(false));

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(true));
        assertThat(presentCtx1.get(), sameInstance(futureCtx1));

        Optional<CompletableFuture<String>> presentCtx2 = loader.getIfPresent("A", "ctx2");
        assertThat(presentCtx2.isPresent(), is(false));

        Optional<CompletableFuture<String>> presentNoCtx = loader.getIfPresent("A");
        assertThat(presentNoCtx.isPresent(), is(false));
    }

    @Test
    public void getIfCompleted_with_context_should_work_before_and_after_dispatch() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        loader.load("A", "ctx1");

        Optional<CompletableFuture<String>> completedBefore = loader.getIfCompleted("A", "ctx1");
        assertThat(completedBefore.isPresent(), is(false));

        loader.dispatch();

        Optional<CompletableFuture<String>> completedAfter = loader.getIfCompleted("A", "ctx1");
        assertThat(completedAfter.isPresent(), is(true));
        assertThat(completedAfter.get().isDone(), is(true));

        Optional<CompletableFuture<String>> completedCtx2 = loader.getIfCompleted("A", "ctx2");
        assertThat(completedCtx2.isPresent(), is(false));
    }

    @Test
    public void clear_with_context_should_only_clear_specified_context() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        CompletableFuture<String> futureCtx2 = loader.load("A", "ctx2");
        loader.dispatch();

        assertThat(futureCtx1.join(), equalTo("A"));
        assertThat(futureCtx2.join(), equalTo("A"));
        assertThat(loadCalls.size(), equalTo(1));

        loader.clear("A", "ctx1");

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(false));

        Optional<CompletableFuture<String>> presentCtx2 = loader.getIfPresent("A", "ctx2");
        assertThat(presentCtx2.isPresent(), is(true));

        CompletableFuture<String> futureCtx1Reload = loader.load("A", "ctx1");
        loader.dispatch();

        assertThat(futureCtx1Reload.join(), equalTo("A"));
        assertThat(loadCalls.size(), equalTo(2));
        assertThat(loadCalls.get(1), equalTo(singletonList("A")));
    }

    @Test
    public void clear_with_context_mapped_batch_loader() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newMappedDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            Map<String, String> map = new java.util.HashMap<>();
            keys.forEach(k -> map.put(k, k));
            return completedFuture(map);
        });

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        CompletableFuture<String> futureCtx2 = loader.load("A", "ctx2");
        loader.dispatch();

        assertThat(futureCtx1.join(), equalTo("A"));
        assertThat(futureCtx2.join(), equalTo("A"));
        assertThat(loadCalls.size(), equalTo(1));

        loader.clear("A", "ctx1");

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(false));

        Optional<CompletableFuture<String>> presentCtx2 = loader.getIfPresent("A", "ctx2");
        assertThat(presentCtx2.isPresent(), is(true));

        CompletableFuture<String> futureCtx1Reload = loader.load("A", "ctx1");
        loader.dispatch();

        assertThat(futureCtx1Reload.join(), equalTo("A"));
        assertThat(loadCalls.size(), equalTo(2));
        assertThat(loadCalls.get(1), equalTo(singletonList("A")));
    }

    @Test
    public void prime_value_with_context_should_only_affect_specified_context() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        loader.prime("A", "ctx1", "primed-ctx1");
        loader.prime("A", "ctx2", "primed-ctx2");

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(true));
        assertThat(presentCtx1.get().join(), equalTo("primed-ctx1"));

        Optional<CompletableFuture<String>> presentCtx2 = loader.getIfPresent("A", "ctx2");
        assertThat(presentCtx2.isPresent(), is(true));
        assertThat(presentCtx2.get().join(), equalTo("primed-ctx2"));

        Optional<CompletableFuture<String>> presentNoCtx = loader.getIfPresent("A");
        assertThat(presentNoCtx.isPresent(), is(false));

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        CompletableFuture<String> futureCtx2 = loader.load("A", "ctx2");
        loader.dispatch();

        assertThat(futureCtx1.join(), equalTo("primed-ctx1"));
        assertThat(futureCtx2.join(), equalTo("primed-ctx2"));
        assertThat(loadCalls.size(), equalTo(0));
    }

    @Test
    public void prime_value_with_context_mapped_batch_loader() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newMappedDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            Map<String, String> map = new java.util.HashMap<>();
            keys.forEach(k -> map.put(k, k));
            return completedFuture(map);
        });

        loader.prime("A", "ctx1", "primed-ctx1");

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(true));
        assertThat(presentCtx1.get().join(), equalTo("primed-ctx1"));

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        loader.dispatch();

        assertThat(futureCtx1.join(), equalTo("primed-ctx1"));
        assertThat(loadCalls.size(), equalTo(0));
    }

    @Test
    public void prime_exception_with_context_should_only_affect_specified_context() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        RuntimeException ex = new RuntimeException("test error");
        loader.prime("A", "ctx1", ex);

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(true));

        try {
            presentCtx1.get().join();
            assertThat("Should have thrown", false);
        } catch (Exception e) {
            assertThat(e.getCause(), equalTo(ex));
        }

        Optional<CompletableFuture<String>> presentCtx2 = loader.getIfPresent("A", "ctx2");
        assertThat(presentCtx2.isPresent(), is(false));

        CompletableFuture<String> futureCtx2 = loader.load("A", "ctx2");
        loader.dispatch();

        assertThat(futureCtx2.join(), equalTo("A"));
        assertThat(loadCalls.size(), equalTo(1));
    }

    @Test
    public void prime_exception_with_context_mapped_batch_loader() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newMappedDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            Map<String, String> map = new java.util.HashMap<>();
            keys.forEach(k -> map.put(k, k));
            return completedFuture(map);
        });

        RuntimeException ex = new RuntimeException("test error");
        loader.prime("A", "ctx1", ex);

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(true));

        try {
            presentCtx1.get().join();
            assertThat("Should have thrown", false);
        } catch (Exception e) {
            assertThat(e.getCause(), equalTo(ex));
        }

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        loader.dispatch();

        assertThat(futureCtx1.isCompletedExceptionally(), is(true));
        assertThat(loadCalls.size(), equalTo(0));
    }

    @Test
    public void prime_completablefuture_with_context() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        CompletableFuture<String> primedFuture = new CompletableFuture<>();
        loader.prime("A", "ctx1", primedFuture);
        primedFuture.complete("async-primed");

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(true));
        assertThat(presentCtx1.get(), sameInstance(primedFuture));

        CompletableFuture<String> futureCtx1 = loader.load("A", "ctx1");
        loader.dispatch();

        assertThat(futureCtx1.join(), equalTo("async-primed"));
        assertThat(loadCalls.size(), equalTo(0));
    }

    @Test
    public void caching_disabled_query_api_always_empty() {
        DataLoaderOptions options = DataLoaderOptions.newOptions().setCachingEnabled(false).build();
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        }, options);

        loader.load("A", "ctx1");
        loader.dispatch();

        Optional<CompletableFuture<String>> presentCtx1 = loader.getIfPresent("A", "ctx1");
        assertThat(presentCtx1.isPresent(), is(false));

        Optional<CompletableFuture<String>> completedCtx1 = loader.getIfCompleted("A", "ctx1");
        assertThat(completedCtx1.isPresent(), is(false));

        Optional<CompletableFuture<String>> presentNoCtx = loader.getIfPresent("A");
        assertThat(presentNoCtx.isPresent(), is(false));

        Optional<CompletableFuture<String>> completedNoCtx = loader.getIfCompleted("A");
        assertThat(completedNoCtx.isPresent(), is(false));
    }

    @Test
    public void getIfCompleted_mapped_batch_loader_before_and_after_dispatch() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newMappedDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            Map<String, String> map = new java.util.HashMap<>();
            keys.forEach(k -> map.put(k, k));
            return completedFuture(map);
        });

        loader.load("A", "ctx1");

        Optional<CompletableFuture<String>> completedBefore = loader.getIfCompleted("A", "ctx1");
        assertThat(completedBefore.isPresent(), is(false));

        loader.dispatch();

        Optional<CompletableFuture<String>> completedAfter = loader.getIfCompleted("A", "ctx1");
        assertThat(completedAfter.isPresent(), is(true));
        assertThat(completedAfter.get().isDone(), is(true));
        assertThat(completedAfter.get().join(), equalTo("A"));

        Optional<CompletableFuture<String>> completedCtx2 = loader.getIfCompleted("A", "ctx2");
        assertThat(completedCtx2.isPresent(), is(false));
    }

    @Test
    public void same_key_same_context_should_share_future() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            return completedFuture(keys);
        });

        CompletableFuture<String> future1 = loader.load("A", "ctx1");
        CompletableFuture<String> future2 = loader.load("A", "ctx1");

        assertThat(future1, sameInstance(future2));

        loader.dispatch();

        assertThat(future1.join(), equalTo("A"));
        assertThat(loadCalls.size(), equalTo(1));
        assertThat(loadCalls.get(0), equalTo(singletonList("A")));
    }

    @Test
    public void same_key_same_context_should_share_future_mapped_batch_loader() {
        List<Collection<String>> loadCalls = new ArrayList<>();
        DataLoader<String, String> loader = DataLoaderFactory.newMappedDataLoader(keys -> {
            loadCalls.add(new ArrayList<>(keys));
            Map<String, String> map = new java.util.HashMap<>();
            keys.forEach(k -> map.put(k, k));
            return completedFuture(map);
        });

        CompletableFuture<String> future1 = loader.load("A", "ctx1");
        CompletableFuture<String> future2 = loader.load("A", "ctx1");

        assertThat(future1, sameInstance(future2));

        loader.dispatch();

        assertThat(future1.join(), equalTo("A"));
        assertThat(loadCalls.size(), equalTo(1));
        assertThat(loadCalls.get(0), equalTo(singletonList("A")));
    }
}
