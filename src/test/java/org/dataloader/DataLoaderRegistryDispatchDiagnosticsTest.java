package org.dataloader;

import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.dataloader.registries.DispatchPredicate;
import org.dataloader.registries.ScheduledDataLoaderRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class DataLoaderRegistryDispatchDiagnosticsTest {
    final BatchLoader<Object, Object> identityBatchLoader = CompletableFuture::completedFuture;

    @Test
    public void dispatchDepths_returns_per_loader_depths() {
        DataLoader<Object, Object> usersDL = newDataLoader("users", identityBatchLoader);
        DataLoader<Object, Object> teamsDL = newDataLoader("teams", identityBatchLoader);
        DataLoader<Object, Object> projectsDL = newDataLoader("projects", identityBatchLoader);

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("users", usersDL);
        registry.register("teams", teamsDL);
        registry.register("projects", projectsDL);

        usersDL.load("U1");
        usersDL.load("U2");
        teamsDL.load("T1");

        Map<String, Integer> depths = registry.dispatchDepths();

        assertThat(depths.get("users"), equalTo(2));
        assertThat(depths.get("teams"), equalTo(1));
        assertThat(depths.get("projects"), equalTo(0));
        assertThat(depths.size(), equalTo(3));
    }

    @Test
    public void dispatchAllWithCounts_returns_per_loader_counts_and_resets_depths() {
        DataLoader<Object, Object> usersDL = newDataLoader("users", identityBatchLoader);
        DataLoader<Object, Object> teamsDL = newDataLoader("teams", identityBatchLoader);
        DataLoader<Object, Object> projectsDL = newDataLoader("projects", identityBatchLoader);

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("users", usersDL);
        registry.register("teams", teamsDL);
        registry.register("projects", projectsDL);

        usersDL.load("U1");
        usersDL.load("U2");
        teamsDL.load("T1");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(counts.get("users"), equalTo(2));
        assertThat(counts.get("teams"), equalTo(1));
        assertThat(counts.get("projects"), equalTo(0));

        Map<String, Integer> depthsAfter = registry.dispatchDepths();
        assertThat(depthsAfter.get("users"), equalTo(0));
        assertThat(depthsAfter.get("teams"), equalTo(0));
        assertThat(depthsAfter.get("projects"), equalTo(0));
    }

    @Test
    public void computeIfAbsent_creates_loader_that_appears_in_diagnostic_maps() {
        DataLoaderRegistry registry = new DataLoaderRegistry();

        DataLoader<Object, Object> dl = registry.computeIfAbsent("dynamic", key -> newDataLoader(key, identityBatchLoader));
        dl.load("K1");
        dl.load("K2");
        dl.load("K3");

        Map<String, Integer> depths = registry.dispatchDepths();
        assertThat(depths, hasKey("dynamic"));
        assertThat(depths.get("dynamic"), equalTo(3));

        Map<String, Integer> counts = registry.dispatchAllWithCounts();
        assertThat(counts, hasKey("dynamic"));
        assertThat(counts.get("dynamic"), equalTo(3));
    }

    @Test
    public void scheduled_registry_dispatchAllWithCounts_respects_predicates() {
        DataLoader<Object, Object> dlA = newDataLoader("A", identityBatchLoader);

        AtomicBoolean bBatchLoaderCalled = new AtomicBoolean(false);
        BatchLoader<Object, Object> trackingBatchLoader = keys -> {
            bBatchLoaderCalled.set(true);
            List<Object> results = new ArrayList<>();
            for (Object key : keys) {
                results.add(key);
            }
            return CompletableFuture.completedFuture(results);
        };
        DataLoader<Object, Object> dlBTracking = newDataLoader("B", trackingBatchLoader);

        DispatchPredicate predicateTrue = (key, dl) -> true;
        DispatchPredicate predicateFalse = (key, dl) -> false;

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("A", dlA, predicateTrue)
                .register("B", dlBTracking, predicateFalse)
                .schedule(Duration.ofHours(1000))
                .build();

        dlA.load("A1");
        dlA.load("A2");
        dlBTracking.load("B1");
        dlBTracking.load("B2");
        dlBTracking.load("B3");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(counts.get("A"), equalTo(2));
        assertThat(counts.get("B"), equalTo(0));

        assertThat(bBatchLoaderCalled.get(), equalTo(false));
    }

    @Test
    public void scheduled_registry_dispatchDepths_respects_predicates() {
        DataLoader<Object, Object> dlA = newDataLoader("A", identityBatchLoader);
        DataLoader<Object, Object> dlB = newDataLoader("B", identityBatchLoader);

        DispatchPredicate predicateTrue = (key, dl) -> true;
        DispatchPredicate predicateFalse = (key, dl) -> false;

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("A", dlA, predicateTrue)
                .register("B", dlB, predicateFalse)
                .schedule(Duration.ofHours(1000))
                .build();

        dlA.load("A1");
        dlA.load("A2");
        dlB.load("B1");
        dlB.load("B2");
        dlB.load("B3");

        Map<String, Integer> depths = registry.dispatchDepths();

        assertThat(depths.get("A"), equalTo(2));
        assertThat(depths.get("B"), equalTo(0));
    }

    @Test
    public void instrumentation_is_triggered_via_dispatchAllWithCounts() {
        AtomicInteger beginDispatchCount = new AtomicInteger(0);
        AtomicInteger beginBatchLoaderCount = new AtomicInteger(0);

        DataLoaderInstrumentation instrumentation = new DataLoaderInstrumentation() {
            @Override
            public DataLoaderInstrumentationContext<Object> beginLoad(DataLoader<?, ?> dataLoader, Object key, Object loadContext) {
                return new DataLoaderInstrumentationContext<Object>() {};
            }

            @Override
            public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                beginDispatchCount.incrementAndGet();
                return new DataLoaderInstrumentationContext<DispatchResult<?>>() {};
            }

            @Override
            public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
                beginBatchLoaderCount.incrementAndGet();
                return new DataLoaderInstrumentationContext<List<?>>() {};
            }
        };

        DataLoader<Object, Object> dlA = newDataLoader("a", identityBatchLoader);
        DataLoader<Object, Object> dlB = newDataLoader("b", identityBatchLoader);

        DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                .register("a", dlA)
                .register("b", dlB)
                .instrumentation(instrumentation)
                .build();

        DataLoader<Object, Object> instrumentedA = registry.getDataLoader("a");
        DataLoader<Object, Object> instrumentedB = registry.getDataLoader("b");

        instrumentedA.load("A1");
        instrumentedB.load("B1");

        registry.dispatchAllWithCounts();

        assertThat(beginDispatchCount.get() > 0, equalTo(true));
        assertThat(beginBatchLoaderCount.get() > 0, equalTo(true));
    }

    @Test
    public void dispatchDepth_is_sum_of_dispatchDepths() {
        DataLoader<Object, Object> dlA = newDataLoader("a", identityBatchLoader);
        DataLoader<Object, Object> dlB = newDataLoader("b", identityBatchLoader);
        DataLoader<Object, Object> dlC = newDataLoader("c", identityBatchLoader);

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("a", dlA);
        registry.register("b", dlB);
        registry.register("c", dlC);

        dlA.load("A1");
        dlA.load("A2");
        dlB.load("B1");

        Map<String, Integer> depths = registry.dispatchDepths();
        int sum = depths.values().stream().mapToInt(Integer::intValue).sum();

        assertThat(registry.dispatchDepth(), equalTo(sum));
        assertThat(registry.dispatchDepth(), equalTo(3));
    }

    @Test
    public void dispatchAllWithCount_is_sum_of_dispatchAllWithCounts() {
        DataLoader<Object, Object> dlA = newDataLoader("a", identityBatchLoader);
        DataLoader<Object, Object> dlB = newDataLoader("b", identityBatchLoader);
        DataLoader<Object, Object> dlC = newDataLoader("c", identityBatchLoader);

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("a", dlA);
        registry.register("b", dlB);
        registry.register("c", dlC);

        dlA.load("A1");
        dlA.load("A2");
        dlB.load("B1");
        dlB.load("B2");
        dlB.load("B3");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();
        int sum = counts.values().stream().mapToInt(Integer::intValue).sum();

        assertThat(sum, equalTo(5));
        assertThat(registry.dispatchAllWithCount(), equalTo(0));
    }

    @Test
    public void key_order_is_preserved_in_diagnostic_maps() {
        DataLoader<Object, Object> dlZ = newDataLoader("z", identityBatchLoader);
        DataLoader<Object, Object> dlA = newDataLoader("a", identityBatchLoader);
        DataLoader<Object, Object> dlM = newDataLoader("m", identityBatchLoader);

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("z", dlZ);
        registry.register("a", dlA);
        registry.register("m", dlM);

        dlZ.load("Z1");
        dlA.load("A1");
        dlM.load("M1");

        Map<String, Integer> depths = registry.dispatchDepths();
        Map<String, Integer> counts = registry.dispatchAllWithCounts();
        
        List<String> depthKeys = new ArrayList<>(depths.keySet());
        List<String> countKeys = new ArrayList<>(counts.keySet());
        
        assertThat(depthKeys, equalTo(countKeys));
        assertThat(depths.size(), equalTo(3));
        assertThat(counts.size(), equalTo(3));
    }
}
