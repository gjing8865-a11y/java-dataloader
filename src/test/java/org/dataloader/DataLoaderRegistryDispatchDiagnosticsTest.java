package org.dataloader;

import org.dataloader.BatchLoaderEnvironment;
import org.dataloader.DispatchResult;
import org.dataloader.instrumentation.ChainedDataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.dataloader.registries.DispatchPredicate;
import org.dataloader.registries.ScheduledDataLoaderRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.dataloader.DataLoaderFactory.newDataLoader;
import static org.dataloader.fixtures.TestKit.idLoader;
import static org.dataloader.fixtures.TestKit.keysAsValues;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DataLoaderRegistryDispatchDiagnosticsTest {

    @Test
    void dispatchDepthsReturnsPerKeyDepths() {
        DataLoader<String, String> usersLoader = newDataLoader("users", keysAsValues());
        DataLoader<String, String> teamsLoader = newDataLoader("teams", keysAsValues());
        DataLoader<String, String> projectsLoader = newDataLoader("projects", keysAsValues());

        usersLoader.load("U1");
        usersLoader.load("U2");

        teamsLoader.load("T1");

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("users", usersLoader);
        registry.register("teams", teamsLoader);
        registry.register("projects", projectsLoader);

        Map<String, Integer> depths = registry.dispatchDepths();

        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("users", 2);
        expected.put("teams", 1);
        expected.put("projects", 0);

        assertThat(depths, equalTo(expected));
        assertThat(registry.dispatchDepth(), equalTo(3));
    }

    @Test
    void dispatchAllWithCountsZerosOutDispatchedLoaderDepth() {
        DataLoader<String, String> usersLoader = newDataLoader("users", keysAsValues());
        DataLoader<String, String> teamsLoader = newDataLoader("teams", keysAsValues());
        DataLoader<String, String> projectsLoader = newDataLoader("projects", keysAsValues());

        usersLoader.load("U1");
        usersLoader.load("U2");

        teamsLoader.load("T1");

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("users", usersLoader);
        registry.register("teams", teamsLoader);
        registry.register("projects", projectsLoader);

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        Map<String, Integer> expectedCounts = new LinkedHashMap<>();
        expectedCounts.put("users", 2);
        expectedCounts.put("teams", 1);
        expectedCounts.put("projects", 0);

        assertThat(counts, equalTo(expectedCounts));

        Map<String, Integer> depthsAfter = registry.dispatchDepths();

        Map<String, Integer> expectedDepthsAfter = new LinkedHashMap<>();
        expectedDepthsAfter.put("users", 0);
        expectedDepthsAfter.put("teams", 0);
        expectedDepthsAfter.put("projects", 0);

        assertThat(depthsAfter, equalTo(expectedDepthsAfter));
    }

    @Test
    void dispatchAllWithCountSumsOverMap() {
        DataLoader<String, String> usersLoader = newDataLoader("users", keysAsValues());
        DataLoader<String, String> teamsLoader = newDataLoader("teams", keysAsValues());

        usersLoader.load("U1");
        usersLoader.load("U2");
        teamsLoader.load("T1");

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register("users", usersLoader);
        registry.register("teams", teamsLoader);

        int count = registry.dispatchAllWithCount();
        assertThat(count, equalTo(3));
    }

    @Test
    void computeIfAbsentLoaderAppearsInDiagnosticMap() {
        DataLoaderRegistry registry = new DataLoaderRegistry();

        registry.computeIfAbsent("users", k -> newDataLoader("users", keysAsValues()));
        registry.computeIfAbsent("teams", k -> newDataLoader("teams", keysAsValues()));

        DataLoader<String, String> usersLoader = registry.getDataLoader("users");
        DataLoader<String, String> teamsLoader = registry.getDataLoader("teams");

        usersLoader.load("U1");
        teamsLoader.load("T1");

        Map<String, Integer> depths = registry.dispatchDepths();

        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("users", 1);
        expected.put("teams", 1);

        assertThat(depths, equalTo(expected));

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        Map<String, Integer> expectedCounts = new LinkedHashMap<>();
        expectedCounts.put("users", 1);
        expectedCounts.put("teams", 1);

        assertThat(counts, equalTo(expectedCounts));
    }

    @Test
    void scheduledRegistryDispatchAllWithCountsRespectsPerLoaderPredicate() {
        List<List<String>> aCalls = new ArrayList<>();
        List<List<String>> bCalls = new ArrayList<>();

        DataLoader<String, String> dlA = newDataLoader("A", keysAsValues(aCalls));
        DataLoader<String, String> dlB = newDataLoader("B", keysAsValues(bCalls));

        dlA.load("A1");
        dlA.load("A2");
        dlB.load("B1");

        DispatchPredicate predicateTrue = (key, dl) -> true;
        DispatchPredicate predicateFalse = (key, dl) -> false;

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("A", dlA, predicateTrue)
                .register("B", dlB, predicateFalse)
                .scheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
                .schedule(java.time.Duration.ofMillis(100))
                .build();

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(counts.get("A"), equalTo(2));
        assertThat(counts.get("B"), equalTo(0));

        assertThat(aCalls, equalTo(asList(asList("A1", "A2"))));
        assertThat(bCalls.size(), equalTo(0));
    }

    @Test
    void scheduledRegistryDefaultPredicateStillApplies() {
        List<List<String>> aCalls = new ArrayList<>();
        List<List<String>> bCalls = new ArrayList<>();

        DataLoader<String, String> dlA = newDataLoader("A", keysAsValues(aCalls));
        DataLoader<String, String> dlB = newDataLoader("B", keysAsValues(bCalls));

        dlA.load("A1");
        dlB.load("B1");
        dlB.load("B2");

        DispatchPredicate alwaysFalse = (key, dl) -> false;

        ScheduledDataLoaderRegistry registry = ScheduledDataLoaderRegistry.newScheduledRegistry()
                .register("A", dlA)
                .register("B", dlB)
                .dispatchPredicate(alwaysFalse)
                .scheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
                .schedule(java.time.Duration.ofMillis(100))
                .build();

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(counts.get("A"), equalTo(0));
        assertThat(counts.get("B"), equalTo(0));

        assertThat(aCalls.size(), equalTo(0));
        assertThat(bCalls.size(), equalTo(0));
    }

    @Test
    void registryInstrumentationFiresOnDispatchAllWithCounts() {
        List<String> methods = new ArrayList<>();
        DataLoaderInstrumentation instr = new DataLoaderInstrumentation() {
            @Override
            public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                methods.add("beginDispatch");
                return new DataLoaderInstrumentationContext<>() {
                    @Override
                    public void onDispatched() {
                        methods.add("beginDispatch_onDispatched");
                    }

                    @Override
                    public void onCompleted(DispatchResult<?> result, Throwable t) {
                        methods.add("beginDispatch_onCompleted");
                    }
                };
            }

            @Override
            public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
                methods.add("beginBatchLoader");
                return new DataLoaderInstrumentationContext<>() {
                    @Override
                    public void onDispatched() {
                        methods.add("beginBatchLoader_onDispatched");
                    }

                    @Override
                    public void onCompleted(List<?> result, Throwable t) {
                        methods.add("beginBatchLoader_onCompleted");
                    }
                };
            }
        };

        DataLoader<String, String> dlA = idLoader("A");
        DataLoader<String, String> dlB = idLoader("B");

        DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                .instrumentation(new ChainedDataLoaderInstrumentation().add(instr))
                .register("A", dlA)
                .register("B", dlB)
                .build();

        DataLoader<String, String> loaderA = registry.getDataLoader("A");
        DataLoader<String, String> loaderB = registry.getDataLoader("B");

        CompletableFuture<String> fA = loaderA.load("X");
        CompletableFuture<String> fB = loaderB.load("Y");

        Map<String, Integer> counts = registry.dispatchAllWithCounts();

        assertThat(counts.get("A"), equalTo(1));
        assertThat(counts.get("B"), equalTo(1));

        assertThat(fA.join(), equalTo("X"));
        assertThat(fB.join(), equalTo("Y"));

        assertThat(methods.contains("beginDispatch"), equalTo(true));
        assertThat(methods.contains("beginBatchLoader"), equalTo(true));
    }
}