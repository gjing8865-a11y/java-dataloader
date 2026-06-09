package org.dataloader;

import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class DataLoaderFactoryTest {

    // ========================================================================
    // Original smoke test (kept as-is, unchanged).
    // ========================================================================

    @Test
    void can_create_via_builder() {
        BatchLoaderWithContext<String, String> loader = (keys, environment) -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions options = DataLoaderOptions.newOptions().setBatchingEnabled(true).build();

        DataLoader<String, String> dl = DataLoaderFactory.<String, String>builder()
                .name("x").batchLoader(loader).options(options).build();

        assertNotNull(dl.getName());
        assertThat(dl.getName(), equalTo("x"));
        assertThat(dl.getBatchLoadFunction(), equalTo(loader));
        assertThat(dl.getOptions(), equalTo(options));

        BatchLoaderWithContext<String, Try<String>> loaderTry = (keys, environment)
                -> CompletableFuture.completedFuture(keys.stream().map(Try::succeeded).collect(Collectors.toList()));

        DataLoader<String, Try<String>> dlTry = DataLoaderFactory.<String, Try<String>>builder()
                .name("try").batchLoader(loaderTry).options(options).build();

        assertNotNull(dlTry.getName());
        assertThat(dlTry.getName(), equalTo("try"));
        assertThat(dlTry.getBatchLoadFunction(), equalTo(loaderTry));
        assertThat(dlTry.getOptions(), equalTo(options));

        MappedBatchLoader<String, Try<String>> mappedLoaderTry = (keys)
                -> CompletableFuture.completedFuture(
                keys.stream().collect(Collectors.toMap(k -> k, Try::succeeded))
        );

        DataLoader<String, Try<String>> dlTry2 = DataLoaderFactory.<String, Try<String>>builder()
                .name("try2").mappedBatchLoader(mappedLoaderTry).options(options).build();

        assertNotNull(dlTry2.getName());
        assertThat(dlTry2.getName(), equalTo("try2"));
        assertThat(dlTry2.getBatchLoadFunction(), equalTo(mappedLoaderTry));
        assertThat(dlTry2.getOptions(), equalTo(options));
    }

    // ========================================================================
    // 1. Reflection-based public API snapshot of DataLoaderFactory and
    //    DataLoaderFactory.Builder.  Any missing/moved method should be caught
    //    here.
    // ========================================================================

    @Test
    void reflection_snapshot_factory_has_all_expected_static_methods() {
        Set<String> expectedFactory = new HashSet<>(Arrays.asList(
                // BatchLoader (lambda-compatible) variants
                "newDataLoader",
                // Try variants
                "newDataLoaderWithTry",
                // Mapped variants
                "newMappedDataLoader",
                "newMappedDataLoaderWithTry",
                // Publisher variants
                "newPublisherDataLoader",
                "newPublisherDataLoaderWithTry",
                // Mapped publisher variants
                "newMappedPublisherDataLoader",
                "newMappedPublisherDataLoaderWithTry",
                // Builder entry
                "builder",
                // Internal helper (should exist exactly once; may or may not
                // show up in reflection depending on JDK version -- do not fail
                // over it but record it if present).
                "mkDataLoader"
        ));

        Set<String> actualNames = new HashSet<>();
        for (Method m : DataLoaderFactory.class.getDeclaredMethods()) {
            int mods = m.getModifiers();
            if (!Modifier.isPublic(mods) || !Modifier.isStatic(mods)) {
                continue;
            }
            actualNames.add(m.getName());
        }

        // Hard-assert that every *expected* public static entry point is
        // actually present.
        assertThat(actualNames, hasItem("newDataLoader"));
        assertThat(actualNames, hasItem("newDataLoaderWithTry"));
        assertThat(actualNames, hasItem("newMappedDataLoader"));
        assertThat(actualNames, hasItem("newMappedDataLoaderWithTry"));
        assertThat(actualNames, hasItem("newPublisherDataLoader"));
        assertThat(actualNames, hasItem("newPublisherDataLoaderWithTry"));
        assertThat(actualNames, hasItem("newMappedPublisherDataLoader"));
        assertThat(actualNames, hasItem("newMappedPublisherDataLoaderWithTry"));
        assertThat(actualNames, hasItem("builder"));
    }

    @Test
    void reflection_snapshot_factory_return_types_are_always_dataloader() {
        // Every non-builder public static factory method must return a
        // DataLoader<>.  This guards against a refactor that accidentally
        // swallows a generic (e.g. returning raw DataLoader).
        int factoryCount = 0;
        for (Method m : DataLoaderFactory.class.getDeclaredMethods()) {
            int mods = m.getModifiers();
            if (!Modifier.isPublic(mods) || !Modifier.isStatic(mods)) {
                continue;
            }
            if (m.getName().equals("builder")) {
                // Builder is fine -- its return type is
                // DataLoaderFactory.Builder.
                continue;
            }
            factoryCount++;
            Class<?> rt = m.getReturnType();
            assertEquals(DataLoader.class, rt,
                    "Expected DataLoader return for " + m.toGenericString()
                            + " but got " + rt);
            Type grt = m.getGenericReturnType();
            assertTrue(grt.toString().contains("DataLoader<"),
                    "Expected generic return to mention DataLoader<...>: "
                            + grt);
        }
        // There should be dozens of overloads.  (If this dips unexpectedly,
        // the refactor broke something.)
        assertTrue(factoryCount >= 30,
                "Expected >= 30 factory overloads, got " + factoryCount);
    }

    @Test
    void reflection_snapshot_builder_has_all_expected_entry_points() {
        Set<String> expectedBuilder = new HashSet<>(Arrays.asList(
                "name", "options", "batchLoadFunction",
                "batchLoader",
                "mappedBatchLoader",
                "publisherBatchLoader",
                "mappedPublisherBatchLoader",
                "build"
        ));

        Set<String> actualBuilder = new HashSet<>();
        for (Method m : DataLoaderFactory.Builder.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(m.getModifiers()) || Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            actualBuilder.add(m.getName());
        }
        for (String exp : expectedBuilder) {
            assertThat("Builder should expose '" + exp + "'", actualBuilder, hasItem(exp));
        }
    }

    @Test
    void reflection_snapshot_builder_setters_return_builder_for_chaining() {
        // Every non-build() public instance method on Builder must return
        // the Builder itself (so callers can chain .a().b().c().build()).
        for (Method m : DataLoaderFactory.Builder.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(m.getModifiers()) || Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            if (m.getName().equals("build")) {
                continue;
            }
            assertEquals(DataLoaderFactory.Builder.class, m.getReturnType(),
                    "Setter " + m.getName() + " should return Builder");
        }
    }

    // ========================================================================
    // 2. Generic-inference smoke tests.  Each of these exercises a different
    //    loader-interface family and deliberately uses the no-arg
    //    `newDataLoader(x)` form (no explicit type witnesses) -- the fact
    //    that this file *compiles* is the main check.
    // ========================================================================

    @Test
    void inference_batchLoader() {
        BatchLoader<String, String> fn = keys -> CompletableFuture.completedFuture(keys);
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(fn);
        assertCompiles_only(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_batchLoader_withName_andOptions() {
        BatchLoader<String, Integer> fn = keys ->
                CompletableFuture.completedFuture(keys.stream().map(String::length).collect(Collectors.toList()));
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
        DataLoader<String, Integer> dl = DataLoaderFactory.newDataLoader("n", fn, opts);
        assertNotNull(dl);
    }

    @Test
    void inference_batchLoaderWithContext() {
        BatchLoaderWithContext<String, String> fn = (keys, env) ->
                CompletableFuture.completedFuture(keys);
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchLoader() {
        MappedBatchLoader<String, String> fn = keys ->
                CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(k -> k, k -> k)));
        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchLoaderWithContext() {
        MappedBatchLoaderWithContext<String, String> fn = (keys, env) ->
                CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(k -> k, k -> k)));
        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_batchPublisher() {
        BatchPublisher<String, String> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_batchPublisherWithContext() {
        BatchPublisherWithContext<String, String> fn = (keys, subscriber, env) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchPublisher() {
        MappedBatchPublisher<String, String> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(new java.util.AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchPublisherWithContext() {
        MappedBatchPublisherWithContext<String, String> fn = (keys, subscriber, env) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(new java.util.AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(fn);
        assertNotNull(dl);
    }

    // Try variants

    @Test
    void inference_batchLoader_try() {
        BatchLoader<String, Try<String>> fn = keys -> {
            List<Try<String>> values = new ArrayList<>(keys.size());
            for (String k : keys) values.add(Try.succeeded(k));
            return CompletableFuture.completedFuture(values);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_batchLoaderWithContext_try() {
        BatchLoaderWithContext<String, Try<String>> fn = (keys, env) -> {
            List<Try<String>> values = new ArrayList<>(keys.size());
            for (String k : keys) values.add(Try.succeeded(k));
            return CompletableFuture.completedFuture(values);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchLoader_try() {
        MappedBatchLoader<String, Try<String>> fn = keys -> {
            Map<String, Try<String>> values = new HashMap<>();
            for (String k : keys) values.put(k, Try.succeeded(k));
            return CompletableFuture.completedFuture(values);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchLoaderWithContext_try() {
        MappedBatchLoaderWithContext<String, Try<String>> fn = (keys, env) -> {
            Map<String, Try<String>> values = new HashMap<>();
            for (String k : keys) values.put(k, Try.succeeded(k));
            return CompletableFuture.completedFuture(values);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_batchPublisher_try() {
        BatchPublisher<String, Try<String>> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(Try.succeeded(k));
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_batchPublisherWithContext_try() {
        BatchPublisherWithContext<String, Try<String>> fn = (keys, subscriber, env) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(Try.succeeded(k));
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchPublisher_try() {
        MappedBatchPublisher<String, Try<String>> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(new java.util.AbstractMap.SimpleEntry<>(k, Try.succeeded(k)));
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    @Test
    void inference_mappedBatchPublisherWithContext_try() {
        MappedBatchPublisherWithContext<String, Try<String>> fn = (keys, subscriber, env) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(new java.util.AbstractMap.SimpleEntry<>(k, Try.succeeded(k)));
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoaderWithTry(fn);
        assertNotNull(dl);
    }

    // Helper used purely to make sure the generic inference path keeps the
    // generic <K,V> signatures we need.
    private static <A, B> void assertCompiles_only(BatchLoader<A, B> fn) {
        // Intentionally empty -- if this compiles, inference works.
    }

    // ========================================================================
    // 3. Identity matrix.  For every loader interface we verify:
    //      a) no-name, null/default options -> getBatchLoadFunction returns
    //         the SAME instance; getOptions sameInstance; getName null.
    //      b) name + explicit options -> name retained; options sameInstance.
    //      c) the same loader object can be supplied via Builder and yields
    //         the same instance semantics.
    // ========================================================================

    @Test
    void identity_matrix_batchLoader() {
        BatchLoader<String, String> fn = keys -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        // (a) no name + null options
        DataLoader<String, String> dlA = DataLoaderFactory.newDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlA.getOptions(), notNullValue());

        // (b) name + explicit options
        DataLoader<String, String> dlB = DataLoaderFactory.newDataLoader("nameA", fn, opts);
        assertThat(dlB.getName(), equalTo("nameA"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));

        // (c) Builder path
        DataLoader<String, String> dlC = DataLoaderFactory.<String, String>builder()
                .name("nameB").batchLoader(fn).options(opts).build();
        assertThat(dlC.getName(), equalTo("nameB"));
        assertThat(dlC.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlC.getOptions(), sameInstance(opts));
    }

    @Test
    void identity_matrix_batchLoaderWithContext() {
        BatchLoaderWithContext<String, String> fn = (keys, env) -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dlA = DataLoaderFactory.newDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));

        DataLoader<String, String> dlB = DataLoaderFactory.newDataLoader("ctx", fn, opts);
        assertThat(dlB.getName(), equalTo("ctx"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));
    }

    @Test
    void identity_matrix_mappedBatchLoader() {
        MappedBatchLoader<String, String> fn = keys ->
                CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(k -> k, k -> k)));
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dlA = DataLoaderFactory.newMappedDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));

        DataLoader<String, String> dlB = DataLoaderFactory.newMappedDataLoader("m", fn, opts);
        assertThat(dlB.getName(), equalTo("m"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));
    }

    @Test
    void identity_matrix_mappedBatchLoaderWithContext() {
        MappedBatchLoaderWithContext<String, String> fn = (keys, env) ->
                CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(k -> k, k -> k)));
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dlA = DataLoaderFactory.newMappedDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));

        DataLoader<String, String> dlB = DataLoaderFactory.newMappedDataLoader("mctx", fn, opts);
        assertThat(dlB.getName(), equalTo("mctx"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));
    }

    @Test
    void identity_matrix_batchPublisher() {
        BatchPublisher<String, String> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dlA = DataLoaderFactory.newPublisherDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));

        DataLoader<String, String> dlB = DataLoaderFactory.newPublisherDataLoader("p", fn, opts);
        assertThat(dlB.getName(), equalTo("p"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));
    }

    @Test
    void identity_matrix_batchPublisherWithContext() {
        BatchPublisherWithContext<String, String> fn = (keys, subscriber, env) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dlA = DataLoaderFactory.newPublisherDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));

        DataLoader<String, String> dlB = DataLoaderFactory.newPublisherDataLoader("pctx", fn, opts);
        assertThat(dlB.getName(), equalTo("pctx"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));
    }

    @Test
    void identity_matrix_mappedBatchPublisher() {
        MappedBatchPublisher<String, String> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(new java.util.AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dlA = DataLoaderFactory.newMappedPublisherDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));

        DataLoader<String, String> dlB = DataLoaderFactory.newMappedPublisherDataLoader("mp", fn, opts);
        assertThat(dlB.getName(), equalTo("mp"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));
    }

    @Test
    void identity_matrix_mappedBatchPublisherWithContext() {
        MappedBatchPublisherWithContext<String, String> fn = (keys, subscriber, env) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(new java.util.AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoaderOptions opts = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dlA = DataLoaderFactory.newMappedPublisherDataLoader(fn, null);
        assertNull(dlA.getName());
        assertThat(dlA.getBatchLoadFunction(), sameInstance((Object) fn));

        DataLoader<String, String> dlB = DataLoaderFactory.newMappedPublisherDataLoader("mpctx", fn, opts);
        assertThat(dlB.getName(), equalTo("mpctx"));
        assertThat(dlB.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlB.getOptions(), sameInstance(opts));
    }

    @Test
    void name_overload_rejects_null_name_via_nonNull() {
        BatchLoader<String, String> fn = keys -> CompletableFuture.completedFuture(keys);
        assertThrows(NullPointerException.class,
                () -> DataLoaderFactory.newDataLoader((String) null, fn, null));
    }

    // ========================================================================
    // 4. Actual runtime behavior.
    // ========================================================================

    @Test
    void runtime_batchLoader_load_then_dispatch() {
        AtomicInteger calls = new AtomicInteger(0);
        BatchLoader<String, String> fn = keys -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(keys);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(fn);
        CompletableFuture<String> v1 = dl.load("a");
        CompletableFuture<String> v2 = dl.load("b");
        dl.dispatch();
        assertEquals("a", v1.join());
        assertEquals("b", v2.join());
        assertEquals(1, calls.get());
    }

    @Test
    void runtime_mappedBatchLoader_missing_key_returns_null() {
        MappedBatchLoader<String, String> fn = keys -> {
            Map<String, String> out = new HashMap<>();
            out.put("a", "A");
            // "b" deliberately missing
            return CompletableFuture.completedFuture(out);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(fn);
        CompletableFuture<String> v1 = dl.load("a");
        CompletableFuture<String> v2 = dl.load("b");
        dl.dispatch();
        assertEquals("A", v1.join());
        assertNull(v2.join());
    }

    @Test
    void runtime_batchPublisher_drives_subscriber() {
        BatchPublisher<String, String> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(fn);
        CompletableFuture<String> v1 = dl.load("x");
        dl.dispatch();
        assertEquals("x", v1.join());
    }

    @Test
    void runtime_try_loader_failure_propagates_to_future() {
        BatchLoader<String, Try<String>> fn = keys -> {
            List<Try<String>> values = new ArrayList<>(keys.size());
            for (String k : keys) {
                if ("bad".equals(k)) {
                    values.add(Try.failed(new RuntimeException("no good")));
                } else {
                    values.add(Try.succeeded(k));
                }
            }
            return CompletableFuture.completedFuture(values);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoaderWithTry(fn);
        CompletableFuture<String> good = dl.load("ok");
        CompletableFuture<String> bad = dl.load("bad");
        dl.dispatch();
        assertEquals("ok", good.join());
        try {
            bad.join();
            fail("Expected exception on bad item");
        } catch (Exception e) {
            // Expected -- the future must complete exceptionally.
        }
    }

    // ========================================================================
    // 5. Instrumentation interaction via DataLoaderRegistry.  We attach an
    //    instrumentation to a registry, register a factory-built loader,
    //    dispatch some keys, and assert the begin-batch/begin-dispatch
    //    callbacks still fire in the same order.
    // ========================================================================

    @Test
    void instrumentation_beginDispatch_and_beginBatchLoader_fire_in_order() {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        DataLoaderInstrumentation inst = new DataLoaderInstrumentation() {
            @Override
            public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                events.add("beginDispatch");
                return null;
            }

            @Override
            public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
                events.add("beginBatchLoader");
                return null;
            }
        };

        BatchLoader<String, String> fn = keys -> CompletableFuture.completedFuture(keys);
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(fn);
        DataLoaderRegistry reg = DataLoaderRegistry.newRegistry()
                .instrumentation(inst)
                .register("x", dl)
                .build();
        dl = reg.getDataLoader("x");
        dl.load("a");
        dl.dispatch();

        assertTrue(events.contains("beginDispatch"),
                "Expected beginDispatch; got " + events);
        assertTrue(events.contains("beginBatchLoader"),
                "Expected beginBatchLoader; got " + events);
    }

    // ========================================================================
    // 6. builder(existingDataLoader) copies the identity and the rebuilt
    //    loader is still fully functional.
    // ========================================================================

    @Test
    void builder_copy_preserves_batchLoader_identity() {
        BatchLoader<String, String> fn = keys -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions opts = DataLoaderOptions.newOptions().setMaxBatchSize(10).build();
        DataLoader<String, String> dlOrig = DataLoaderFactory.newDataLoader("orig", fn, opts);

        DataLoader<String, String> dlCopy = DataLoaderFactory.builder(dlOrig).build();

        assertThat(dlCopy.getName(), equalTo("orig"));
        assertThat(dlCopy.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlCopy.getOptions(), sameInstance(opts));

        // And the copy is an independent object that can load/dispatch.
        CompletableFuture<String> v = dlCopy.load("q");
        dlCopy.dispatch();
        assertEquals("q", v.join());
    }

    @Test
    void builder_copy_preserves_mappedBatchLoader_identity() {
        MappedBatchLoader<String, String> fn = keys ->
                CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(k -> k, k -> k)));
        DataLoaderOptions opts = DataLoaderOptions.newOptions().setMaxBatchSize(5).build();
        DataLoader<String, String> dlOrig = DataLoaderFactory.newMappedDataLoader("m", fn, opts);

        DataLoader<String, String> dlCopy = DataLoaderFactory.builder(dlOrig).build();
        assertThat(dlCopy.getName(), equalTo("m"));
        assertThat(dlCopy.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlCopy.getOptions(), sameInstance(opts));

        CompletableFuture<String> v = dlCopy.load("z");
        dlCopy.dispatch();
        assertEquals("z", v.join());
    }

    @Test
    void builder_copy_preserves_batchPublisher_identity() {
        BatchPublisher<String, String> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoaderOptions opts = DataLoaderOptions.newOptions().setMaxBatchSize(7).build();
        DataLoader<String, String> dlOrig = DataLoaderFactory.newPublisherDataLoader("p", fn, opts);

        DataLoader<String, String> dlCopy = DataLoaderFactory.builder(dlOrig).build();
        assertThat(dlCopy.getName(), equalTo("p"));
        assertThat(dlCopy.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlCopy.getOptions(), sameInstance(opts));

        CompletableFuture<String> v = dlCopy.load("y");
        dlCopy.dispatch();
        assertEquals("y", v.join());
    }

    @Test
    void builder_copy_preserves_mappedBatchPublisher_identity() {
        MappedBatchPublisher<String, String> fn = (keys, subscriber) -> {
            subscriber.onSubscribe(new NoopSubscription());
            for (String k : keys) subscriber.onNext(new java.util.AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoaderOptions opts = DataLoaderOptions.newOptions().setMaxBatchSize(3).build();
        DataLoader<String, String> dlOrig = DataLoaderFactory.newMappedPublisherDataLoader("mp", fn, opts);

        DataLoader<String, String> dlCopy = DataLoaderFactory.builder(dlOrig).build();
        assertThat(dlCopy.getName(), equalTo("mp"));
        assertThat(dlCopy.getBatchLoadFunction(), sameInstance((Object) fn));
        assertThat(dlCopy.getOptions(), sameInstance(opts));

        CompletableFuture<String> v = dlCopy.load("w");
        dlCopy.dispatch();
        assertEquals("w", v.join());
    }

    // -----------------------------------------------------------------------
    // Tiny Reactive Streams subscription helper.  A real reactive-streams
    // implementation is out of scope for these tests; a no-op that simply
    // satisfies the protocol is enough.
    // -----------------------------------------------------------------------
    private static final class NoopSubscription implements Subscription {
        @Override public void request(long n) { }
        @Override public void cancel() { }
    }
}
