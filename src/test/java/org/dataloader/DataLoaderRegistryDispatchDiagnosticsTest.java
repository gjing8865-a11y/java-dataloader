package org.dataloader;

import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.dataloader.instrumentation.DataLoaderInstrumentationHelper;
import org.dataloader.registries.DispatchPredicate;
import org.dataloader.registries.ScheduledDataLoaderRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DataLoaderRegistryDispatchDiagnosticsTest {

    @Test
    void registryDispatchDiagnosticsReportPerKeyDepthsAndCountsInStableOrder() {
        DataLoaderRegistry registry = new DataLoaderRegistry();

        DataLoader<String, String> users = registry.registerAndGet("users", identityDataLoader());
        DataLoader<String, String> teams = registry.registerAndGet("teams", identityDataLoader());
        registry.computeIfAbsent("projects", key -> identityDataLoader());

        users.load("u1");
        users.load("u2");
        teams.load("t1");

        Map<String, Integer> dispatchDepths = registry.dispatchDepths();
        assertThat(new ArrayList<>(dispatchDepths.keySet()), equalTo(List.of("users", "teams", "projects")));
        assertThat(dispatchDepths, equalTo(orderedMapOf(
                Map.entry("users", 2),
                Map.entry("teams", 1),
                Map.entry("projects", 0)
        )));

        Map<String, Integer> dispatchCounts = registry.dispatchAllWithCounts();
        assertThat(new ArrayList<>(dispatchCounts.keySet()), equalTo(List.of("users", "teams", "projects")));
        assertThat(dispatchCounts, equalTo(orderedMapOf(
                Map.entry("users", 2),
                Map.entry("teams", 1),
                Map.entry("projects", 0)
        )));
    }

    @Test
    void dispatchAllWithCountsClearsDepthForDispatchedLoaders() {
        DataLoaderRegistry registry = new DataLoaderRegistry();

        DataLoader<String, String> users = registry.registerAndGet("users", identityDataLoader());
        DataLoader<String, String> teams = registry.registerAndGet("teams", identityDataLoader());

        users.load("u1");
        users.load("u2");
        teams.load("t1");

        assertThat(registry.dispatchAllWithCounts(), equalTo(orderedMapOf(
                Map.entry("users", 2),
                Map.entry("teams", 1)
        )));
        assertThat(registry.dispatchDepths(), equalTo(orderedMapOf(
                Map.entry("users", 0),
                Map.entry("teams", 0)
        )));
    }

    @Test
    void scheduledRegistryDispatchDiagnosticsRespectPerLoaderPredicates() {
        DispatchPredicate alwaysDispatch = (key, dataLoader) -> true;
        DispatchPredicate neverDispatch = (key, dataLoader) -> false;

        List<List<String>> aCalls = new ArrayList<>();
        List<List<String>> bCalls = new ArrayList<>();

        try (ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("A", identityDataLoader(aCalls))
                .register("B", identityDataLoader(bCalls), neverDispatch)
                .dispatchPredicate(alwaysDispatch)
                .schedule(Duration.ofHours(1))
                .build()) {

            DataLoader<String, String> loaderA = registry.getDataLoader("A");
            DataLoader<String, String> loaderB = registry.getDataLoader("B");

            CompletableFuture<String> futureA = loaderA.load("a-1");
            CompletableFuture<String> futureB = loaderB.load("b-1");

            Map<String, Integer> dispatchCounts = registry.dispatchAllWithCounts();
            assertThat(new ArrayList<>(dispatchCounts.keySet()), equalTo(List.of("A", "B")));
            assertThat(dispatchCounts, equalTo(orderedMapOf(
                    Map.entry("A", 1),
                    Map.entry("B", 0)
            )));
            assertThat(registry.dispatchDepths(), equalTo(orderedMapOf(
                    Map.entry("A", 0),
                    Map.entry("B", 1)
            )));
            assertThat(futureA.join(), equalTo("a-1"));
            assertThat(futureB.isDone(), equalTo(false));
            assertThat(aCalls, equalTo(List.of(List.of("a-1"))));
            assertThat(bCalls, equalTo(List.of()));
        }
    }

    @Test
    void dispatchAllWithCountsStillTriggersRegistryInstrumentation() {
        CountingInstrumentation instrumentation = new CountingInstrumentation();

        DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                .instrumentation(instrumentation)
                .register("users", identityDataLoader())
                .build();

        DataLoader<String, String> users = registry.getDataLoader("users");
        users.load("u1");

        assertThat(registry.dispatchAllWithCounts(), equalTo(orderedMapOf(Map.entry("users", 1))));
        assertThat(instrumentation.beginDispatchCount.get(), equalTo(1));
        assertThat(instrumentation.beginBatchLoaderCount.get(), equalTo(1));
    }

    private static DataLoader<String, String> identityDataLoader() {
        return identityDataLoader(new ArrayList<>());
    }

    private static DataLoader<String, String> identityDataLoader(List<List<String>> calls) {
        return newDataLoader(keys -> {
            calls.add(new ArrayList<>(keys));
            return CompletableFuture.completedFuture(new ArrayList<>(keys));
        });
    }

    @SafeVarargs
    private static LinkedHashMap<String, Integer> orderedMapOf(Map.Entry<String, Integer>... entries) {
        LinkedHashMap<String, Integer> orderedMap = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : entries) {
            orderedMap.put(entry.getKey(), entry.getValue());
        }
        return orderedMap;
    }

    private static class CountingInstrumentation implements DataLoaderInstrumentation {
        private final AtomicInteger beginDispatchCount = new AtomicInteger();
        private final AtomicInteger beginBatchLoaderCount = new AtomicInteger();

        @Override
        public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
            beginDispatchCount.incrementAndGet();
            return DataLoaderInstrumentationHelper.noOpCtx();
        }

        @Override
        public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
            beginBatchLoaderCount.incrementAndGet();
            return DataLoaderInstrumentationHelper.noOpCtx();
        }
    }
}
