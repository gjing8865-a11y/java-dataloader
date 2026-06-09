package org.dataloader;

import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.dataloader.registries.DispatchPredicate;
import org.dataloader.registries.ScheduledDataLoaderRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;

public class DataLoaderRegistryDispatchDiagnosticsTest {

    static class TrackingInstrumentation implements DataLoaderInstrumentation {
        final List<String> calls = new ArrayList<>();
        final String name;

        TrackingInstrumentation(String name) {
            this.name = name;
        }

        @Override
        public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
            calls.add(name + "_beginDispatch");
            return new DataLoaderInstrumentationContext<>() {
                @Override
                public void onDispatched() {
                    calls.add(name + "_beginDispatch_onDispatched");
                }

                @Override
                public void onCompleted(DispatchResult<?> result, Throwable t) {
                    calls.add(name + "_beginDispatch_onCompleted");
                }
            };
        }

        @Override
        public DataLoaderInstrumentationContext<java.util.List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, java.util.List<?> keys, BatchLoaderEnvironment environment) {
            calls.add(name + "_beginBatchLoader");
            return new DataLoaderInstrumentationContext<>() {
                @Override
                public void onDispatched() {
                    calls.add(name + "_beginBatchLoader_onDispatched");
                }

                @Override
                public void onCompleted(java.util.List<?> result, Throwable t) {
                    calls.add(name + "_beginBatchLoader_onCompleted");
                }
            };
        }
    }

    private final BatchLoader<String, String> identityBatchLoader = keys ->
            CompletableFuture.completedFuture(new ArrayList<>(keys));

    @Test
    public void dispatchDepths_reports_per_loader_depth_in_registration_order() {
        DataLoaderRegistry registry = new DataLoaderRegistry();

        DataLoader<String, String> users = registry.registerAndGet("users",
                newDataLoader(identityBatchLoader));
        DataLoader<String, String> teams = registry.registerAndGet("teams",
                newDataLoader(identityBatchLoader));
        DataLoader<String, String> projects = registry.registerAndGet("projects",
                newDataLoader(identityBatchLoader));

        users.load("u1");
        users.load("u2");
        teams.load("t1");

        Map<String, Integer> depths = registry.dispatchDepths();

        assertThat(new ArrayList<>(depths.keySet()), equalTo(asList("projects", "teams", "users")));
        assertThat(depths.get("users"), equalTo(2));
        assertThat(depths.get("teams"), equalTo(1));
        assertThat(depths.get("projects"), equalTo(0));
    }

    @Test
    public void dispatchAllWithCounts_returns_per_loader_dispatched_count_and_clears_depth() {
        DataLoaderRegistry registry = new DataLoaderRegistry();

        DataLoader<String, String> users = registry.registerAndGet("users",
                newDataLoader(identityBatchLoader));
        DataLoader<String, String> teams = registry.registerAndGet("teams",
                newDataLoader(identityBatchLoader));
        DataLoader<String, String> projects = registry.registerAndGet("projects",
                newDataLoader(identityBatchLoader));

        users.load("u1");
        users.load("u2");
        teams.load("t1");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(new ArrayList<>(counts.keySet()), equalTo(asList("projects", "teams", "users")));
        assertThat(counts.get("users"), equalTo(2));
        assertThat(counts.get("teams"), equalTo(1));
        assertThat(counts.get("projects"), equalTo(0));

        Map<String, Integer> depths = registry.dispatchDepths();
        assertThat(depths.get("users"), equalTo(0));
        assertThat(depths.get("teams"), equalTo(0));
        assertThat(depths.get("projects"), equalTo(0));

        assertThat(registry.dispatchDepth(), equalTo(0));
        assertThat(registry.dispatchAllWithCount(), equalTo(0));
    }

    @Test
    public void computeIfAbsent_created_loader_appears_in_diagnostic_maps() {
        DataLoaderRegistry registry = new DataLoaderRegistry();

        DataLoader<String, String> users = registry.computeIfAbsent("users",
                key -> newDataLoader(key, identityBatchLoader));
        users.load("u1");
        users.load("u2");

        Map<String, Integer> depths = registry.dispatchDepths();
        assertThat(depths, hasKey("users"));
        assertThat(depths.get("users"), equalTo(2));

        Map<String, Integer> counts = registry.dispatchAllWithCounts();
        assertThat(counts, hasKey("users"));
        assertThat(counts.get("users"), equalTo(2));
    }

    @Test
    public void scheduled_registry_respects_per_loader_predicate_and_returns_zero_for_skipped_loaders() {
        List<Collection<String>> aCalls = new ArrayList<>();
        List<Collection<String>> bCalls = new ArrayList<>();
        AtomicInteger bCounter = new AtomicInteger();

        DataLoader<String, String> dlA = newDataLoader("A", keys -> {
            aCalls.add(keys);
            return CompletableFuture.completedFuture(new ArrayList<>(keys));
        });
        DataLoader<String, String> dlB = newDataLoader("B", keys -> {
            bCalls.add(keys);
            return CompletableFuture.completedFuture(new ArrayList<>(keys));
        });

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("A", dlA, (key, dl) -> true)
                .register("B", dlB, (key, dl) -> {
                    bCounter.incrementAndGet();
                    return false;
                })
                .scheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
                .schedule(Duration.ofMillis(100))
                .build();

        dlA.load("AK1");
        dlA.load("AK2");
        dlB.load("BK1");
        dlB.load("BK2");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(new ArrayList<>(counts.keySet()), equalTo(asList("A", "B")));
        assertThat(counts.get("A"), equalTo(2));
        assertThat(counts.get("B"), equalTo(0));

        assertThat(aCalls.size(), equalTo(1));
        assertThat(bCalls.size(), equalTo(0));

        assertThat(bCounter.get(), equalTo(1));
    }

    @Test
    public void registry_instrumentation_fires_on_dispatchAllWithCounts() {
        TrackingInstrumentation instrumentation = new TrackingInstrumentation("REG");

        DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                .instrumentation(instrumentation)
                .register("users", newDataLoader(identityBatchLoader))
                .build();

        DataLoader<String, String> users = registry.getDataLoader("users");
        users.load("u1");
        users.load("u2");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(counts.get("users"), equalTo(2));
        assertThat(instrumentation.calls, hasItems(
                "REG_beginDispatch",
                "REG_beginBatchLoader",
                "REG_beginDispatch_onDispatched",
                "REG_beginBatchLoader_onDispatched",
                "REG_beginDispatch_onCompleted",
                "REG_beginBatchLoader_onCompleted"
        ));
    }
}
