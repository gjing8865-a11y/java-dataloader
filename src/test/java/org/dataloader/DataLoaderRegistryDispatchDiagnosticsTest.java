package org.dataloader;

import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.dataloader.registries.ScheduledDataLoaderRegistry;
import org.junit.jupiter.api.Test;
import org.dataloader.BatchLoaderEnvironment;
import org.dataloader.DispatchResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataLoaderRegistryDispatchDiagnosticsTest {

    private <K, V> DataLoader<K, V> createLoader() {
        BatchLoader<K, V> batchLoader = keys -> CompletableFuture.completedFuture(null);
        return DataLoaderFactory.newDataLoader(batchLoader);
    }

    private <K, V> DataLoader<K, V> createLoader(List<List<K>> calls) {
        BatchLoader<K, V> batchLoader = keys -> {
            calls.add(keys);
            return CompletableFuture.completedFuture(null);
        };
        return DataLoaderFactory.newDataLoader(batchLoader);
    }

    @Test
    public void testRegularRegistryDispatchDepthsAndCounts() {
        DataLoader<String, String> usersLoader = createLoader();
        DataLoader<String, String> teamsLoader = createLoader();
        DataLoader<String, String> projectsLoader = createLoader();

        DataLoaderRegistry registry = new DataLoaderRegistry()
                .register("users", usersLoader)
                .register("teams", teamsLoader)
                .register("projects", projectsLoader);

        DataLoader<String, String> u = registry.getDataLoader("users");
        u.load("u1");
        u.load("u2");
        DataLoader<String, String> t = registry.getDataLoader("teams");
        t.load("t1");

        Map<String, Integer> depths = registry.dispatchDepths();
        assertEquals(3, depths.size());
        
        // order should be stable (alphabetical because we sorted by key)
        List<String> keys = new ArrayList<>(depths.keySet());
        assertEquals(Arrays.asList("projects", "teams", "users"), keys);

        assertEquals(0, depths.get("projects"));
        assertEquals(1, depths.get("teams"));
        assertEquals(2, depths.get("users"));
        assertEquals(3, registry.dispatchDepth());

        Map<String, Integer> counts = registry.dispatchAllWithCounts();
        assertEquals(3, counts.size());
        assertEquals(0, counts.get("projects"));
        assertEquals(1, counts.get("teams"));
        assertEquals(2, counts.get("users"));

        // Load again to test dispatchAllWithCount
        u.load("u3");
        u.load("u4");
        t.load("t2");
        assertEquals(3, registry.dispatchAllWithCount());

        // depths of dispatched loaders become 0
        Map<String, Integer> depthsAfter = registry.dispatchDepths();
        assertEquals(0, depthsAfter.get("projects"));
        assertEquals(0, depthsAfter.get("teams"));
        assertEquals(0, depthsAfter.get("users"));
        assertEquals(0, registry.dispatchDepth());
    }

    @Test
    public void testComputeIfAbsentAppearsInMap() {
        DataLoaderRegistry registry = new DataLoaderRegistry();
        
        registry.computeIfAbsent("dynamicLoader", key -> createLoader());
        DataLoader<String, String> loader = registry.getDataLoader("dynamicLoader");
        loader.load("k1");
        
        Map<String, Integer> depths = registry.dispatchDepths();
        assertEquals(1, depths.size());
        assertEquals(1, depths.get("dynamicLoader"));
    }

    @Test
    public void testScheduledRegistryDispatchAllWithCounts() {
        List<List<String>> callsA = new ArrayList<>();
        DataLoader<String, String> loaderA = createLoader(callsA);
        
        List<List<String>> callsB = new ArrayList<>();
        DataLoader<String, String> loaderB = createLoader(callsB);

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("A", loaderA, (key, dl) -> true) // predicate=true
                .register("B", loaderB, (key, dl) -> false) // predicate=false
                .schedule(Duration.ofMillis(100))
                .build();

        loaderA.load("a1");
        loaderB.load("b1");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();
        
        assertEquals(2, counts.size());
        assertEquals(1, counts.get("A"));
        assertEquals(0, counts.get("B"));
        
        assertEquals(1, callsA.size());
        assertEquals(0, callsB.size());

        registry.close();
    }

    @Test
    public void testInstrumentationStillTriggers() {
        List<String> events = new ArrayList<>();
        DataLoaderInstrumentation instrumentation = new DataLoaderInstrumentation() {
            @Override
            public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                events.add("beginDispatch");
                return new DataLoaderInstrumentationContext<DispatchResult<?>>() {
                    @Override
                    public void onDispatched() {
                        events.add("onDispatched");
                    }

                    @Override
                    public void onCompleted(DispatchResult<?> result, Throwable t) {
                        events.add("onCompleted");
                    }
                };
            }

            @Override
            public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
                events.add("beginBatchLoader");
                return new DataLoaderInstrumentationContext<List<?>>() {
                    @Override
                    public void onDispatched() {
                    }

                    @Override
                    public void onCompleted(List<?> result, Throwable t) {
                    }
                };
            }
        };

        DataLoader<String, String> loader = createLoader();
        DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                .instrumentation(instrumentation)
                .register("testLoader", loader)
                .build();

        registry.<String, String>getDataLoader("testLoader").load("k1");
        
        Map<String, Integer> counts = registry.dispatchAllWithCounts();
        assertEquals(1, counts.get("testLoader"));
        
        assertTrue(events.contains("beginDispatch"));
        assertTrue(events.contains("beginBatchLoader"));
    }
}
