package org.dataloader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.dataloader.DataLoaderFactory.newMappedDataLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataLoaderContextCacheTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("loaderVariants")
    public void same_key_with_different_contexts_do_not_share_future(LoaderVariant variant) {
        List<List<String>> batchCalls = new ArrayList<>();
        DataLoader<String, String> loader = variant.create(contextAwareOptions(), batchCalls);

        CompletableFuture<String> first = loader.load("A", "ctx-1");
        CompletableFuture<String> second = loader.load("A", "ctx-2");

        assertThat(first, not(sameInstance(second)));

        Optional<CompletableFuture<String>> firstCached = loader.getIfPresent("A", "ctx-1");
        Optional<CompletableFuture<String>> secondCached = loader.getIfPresent("A", "ctx-2");
        assertThat(firstCached.isPresent(), is(true));
        assertThat(secondCached.isPresent(), is(true));
        assertThat(firstCached.get(), sameInstance(first));
        assertThat(secondCached.get(), sameInstance(second));

        loader.dispatch().join();

        assertThat(first.join(), equalTo("A"));
        assertThat(second.join(), equalTo("A"));
        assertThat(batchCalls, equalTo(expectedCalls(variant, Arrays.asList(Arrays.asList("A", "A")))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("loaderVariants")
    public void clear_only_clears_the_specified_context(LoaderVariant variant) {
        List<List<String>> batchCalls = new ArrayList<>();
        DataLoader<String, String> loader = variant.create(contextAwareOptions(), batchCalls);

        CompletableFuture<String> firstCtx1 = loader.load("A", "ctx-1");
        CompletableFuture<String> firstCtx2 = loader.load("A", "ctx-2");
        loader.dispatch().join();

        AtomicBoolean handlerCalled = new AtomicBoolean();
        AtomicReference<Throwable> clearError = new AtomicReference<>();
        loader.clear("A", "ctx-1", (ignored, error) -> {
            handlerCalled.set(true);
            clearError.set(error);
        });

        assertThat(handlerCalled.get(), is(true));
        assertThat(clearError.get(), nullValue());
        assertThat(loader.getIfPresent("A", "ctx-1").isPresent(), is(false));
        assertThat(loader.getIfPresent("A", "ctx-2").isPresent(), is(true));
        assertThat(loader.getIfPresent("A", "ctx-2").get(), sameInstance(firstCtx2));

        CompletableFuture<String> secondCtx1 = loader.load("A", "ctx-1");
        CompletableFuture<String> secondCtx2 = loader.load("A", "ctx-2");

        assertThat(secondCtx1, not(sameInstance(firstCtx1)));
        assertThat(secondCtx2, sameInstance(firstCtx2));

        loader.dispatch().join();

        assertThat(batchCalls, equalTo(expectedCalls(variant, Arrays.asList(Arrays.asList("A", "A"), singletonList("A")))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("loaderVariants")
    public void prime_value_only_affects_the_specified_context(LoaderVariant variant) {
        List<List<String>> batchCalls = new ArrayList<>();
        DataLoader<String, String> loader = variant.create(contextAwareOptions(), batchCalls);

        loader.prime("A", "ctx-1", "primed");

        CompletableFuture<String> primedFuture = loader.load("A", "ctx-1");
        CompletableFuture<String> loadedFuture = loader.load("A", "ctx-2");

        loader.dispatch().join();

        assertThat(primedFuture.join(), equalTo("primed"));
        assertThat(loadedFuture.join(), equalTo("A"));
        assertThat(batchCalls, equalTo(expectedCalls(variant, Arrays.asList(singletonList("A")))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("loaderVariants")
    public void prime_exception_only_affects_the_specified_context(LoaderVariant variant) {
        List<List<String>> batchCalls = new ArrayList<>();
        DataLoader<String, String> loader = variant.create(contextAwareOptions(), batchCalls);

        loader.prime("A", "ctx-1", new IllegalStateException("boom"));

        CompletableFuture<String> failedFuture = loader.load("A", "ctx-1");
        CompletableFuture<String> loadedFuture = loader.load("A", "ctx-2");

        loader.dispatch().join();

        CompletionException completionException = assertThrows(CompletionException.class, failedFuture::join);
        assertThat(completionException.getCause(), instanceOf(IllegalStateException.class));
        assertThat(completionException.getCause().getMessage(), equalTo("boom"));
        assertThat(loadedFuture.join(), equalTo("A"));
        assertThat(batchCalls, equalTo(expectedCalls(variant, Arrays.asList(singletonList("A")))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("loaderVariants")
    public void get_if_completed_tracks_dispatch_state_for_context_aware_entries(LoaderVariant variant) {
        List<List<String>> batchCalls = new ArrayList<>();
        DataLoader<String, String> loader = variant.create(contextAwareOptions(), batchCalls);

        CompletableFuture<String> pendingFuture = loader.load("A", "ctx-1");

        assertThat(loader.getIfPresent("A", "ctx-1").isPresent(), is(true));
        assertThat(loader.getIfCompleted("A", "ctx-1").isPresent(), is(false));

        loader.dispatch().join();

        Optional<CompletableFuture<String>> completedFuture = loader.getIfCompleted("A", "ctx-1");
        assertThat(completedFuture.isPresent(), is(true));
        assertThat(completedFuture.get(), sameInstance(pendingFuture));
        assertThat(completedFuture.get().isDone(), is(true));
        assertThat(batchCalls, equalTo(expectedCalls(variant, Arrays.asList(singletonList("A")))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("loaderVariants")
    public void query_apis_are_always_empty_when_caching_is_disabled(LoaderVariant variant) {
        List<List<String>> batchCalls = new ArrayList<>();
        DataLoaderOptions options = DataLoaderOptions.newOptions()
                .setCacheKeyFunction(contextAwareCacheKey())
                .setCachingEnabled(false)
                .build();
        DataLoader<String, String> loader = variant.create(options, batchCalls);

        loader.prime("A", "ctx-1", "primed");

        assertThat(loader.getIfPresent("A", "ctx-1").isPresent(), is(false));
        assertThat(loader.getIfCompleted("A", "ctx-1").isPresent(), is(false));

        CompletableFuture<String> future = loader.load("A", "ctx-1");

        assertThat(loader.getIfPresent("A", "ctx-1").isPresent(), is(false));
        assertThat(loader.getIfCompleted("A", "ctx-1").isPresent(), is(false));

        loader.dispatch().join();

        assertThat(future.join(), equalTo("A"));
        assertThat(loader.getIfPresent("A", "ctx-1").isPresent(), is(false));
        assertThat(loader.getIfCompleted("A", "ctx-1").isPresent(), is(false));
        assertThat(batchCalls, equalTo(expectedCalls(variant, Arrays.asList(singletonList("A")))));
    }

    private static Stream<Arguments> loaderVariants() {
        return Stream.of(
                Arguments.of(new LoaderVariant("BatchLoader", false, (options, batchCalls) -> newDataLoader(keys -> {
                    batchCalls.add(new ArrayList<>(keys));
                    return completedFuture(new ArrayList<>(keys));
                }, options))),
                Arguments.of(new LoaderVariant("MappedBatchLoader", true, (options, batchCalls) -> newMappedDataLoader(keys -> {
                    batchCalls.add(new ArrayList<>(keys));
                    Map<String, String> values = new LinkedHashMap<>();
                    keys.forEach(key -> values.put(key, key));
                    return completedFuture(values);
                }, options)))
        );
    }

    private static DataLoaderOptions contextAwareOptions() {
        return DataLoaderOptions.newOptions()
                .setCacheKeyFunction(contextAwareCacheKey())
                .build();
    }

    private static CacheKey<String> contextAwareCacheKey() {
        return new CacheKey<String>() {
            @Override
            public Object getKey(String input) {
                return input;
            }

            @Override
            public Object getKeyWithContext(String input, Object context) {
                return input + "::" + context;
            }
        };
    }

    private static List<List<String>> expectedCalls(LoaderVariant variant, List<List<String>> rawCalls) {
        return rawCalls.stream()
                .map(keys -> variant.deduplicatesKeys ? new ArrayList<>(new LinkedHashSet<>(keys)) : new ArrayList<>(keys))
                .collect(Collectors.toList());
    }

    private interface LoaderFactory {
        DataLoader<String, String> create(DataLoaderOptions options, List<List<String>> batchCalls);
    }

    private static class LoaderVariant {
        private final String name;
        private final boolean deduplicatesKeys;
        private final LoaderFactory factory;

        private LoaderVariant(String name, boolean deduplicatesKeys, LoaderFactory factory) {
            this.name = name;
            this.deduplicatesKeys = deduplicatesKeys;
            this.factory = factory;
        }

        private DataLoader<String, String> create(DataLoaderOptions options, List<List<String>> batchCalls) {
            return factory.create(options, batchCalls);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
