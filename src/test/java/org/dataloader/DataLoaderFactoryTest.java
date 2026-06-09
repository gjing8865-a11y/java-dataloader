package org.dataloader;

import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.lang.reflect.Method;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

class DataLoaderFactoryTest {

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

    @Test
    void publicApiReflectionSnapshot_allMethodsExist() {
        Set<String> expectedMethodNames = new HashSet<>(Arrays.asList(
                "newDataLoader", "newDataLoaderWithTry",
                "newMappedDataLoader", "newMappedDataLoaderWithTry",
                "newPublisherDataLoader", "newPublisherDataLoaderWithTry",
                "newMappedPublisherDataLoader", "newMappedPublisherDataLoaderWithTry",
                "builder"
        ));

        Method[] methods = DataLoaderFactory.class.getMethods();
        Set<String> actualMethodNames = Arrays.stream(methods)
                .map(Method::getName)
                .filter(expectedMethodNames::contains)
                .collect(Collectors.toSet());

        assertThat(actualMethodNames, equalTo(expectedMethodNames));

        int expectedTotalMethods = countExpectedPublicStaticMethods();
        long actualCount = Arrays.stream(methods)
                .filter(m -> m.getName().startsWith("new") || m.getName().equals("builder"))
                .count();

        assertTrue(actualCount >= expectedTotalMethods,
                "Expected at least " + expectedTotalMethods + " public factory methods, got " + actualCount);

        Class<?> builderClass = Arrays.stream(DataLoaderFactory.class.getDeclaredClasses())
                .filter(c -> c.getSimpleName().equals("Builder"))
                .findFirst()
                .orElseThrow();

        Set<String> expectedBuilderMethods = new HashSet<>(Arrays.asList(
                "name", "options", "batchLoadFunction",
                "batchLoader", "mappedBatchLoader",
                "publisherBatchLoader", "mappedPublisherBatchLoader",
                "build"
        ));

        Set<String> actualBuilderMethods = Arrays.stream(builderClass.getMethods())
                .map(Method::getName)
                .collect(Collectors.toSet());

        expectedBuilderMethods.forEach(expected ->
                assertTrue(actualBuilderMethods.contains(expected),
                        "Builder missing expected method: " + expected));
    }

    private int countExpectedPublicStaticMethods() {
        return 8 * 4;
    }

    @Test
    void genericInference_allTypesInferCorrectly_rawTypesNotUsed() {
        DataLoader<String, String> dl1 = DataLoaderFactory.newDataLoader(keys -> CompletableFuture.completedFuture(keys));
        assertNotNull(dl1);

        DataLoader<String, String> dl2 = DataLoaderFactory.newDataLoader("name", keys -> CompletableFuture.completedFuture(keys));
        assertNotNull(dl2);

        BatchLoader<String, Try<String>> tryLoader = keys ->
                CompletableFuture.completedFuture(keys.stream().map(Try::succeeded).collect(Collectors.toList()));
        DataLoader<String, String> dl3 = DataLoaderFactory.newDataLoaderWithTry(tryLoader);
        assertNotNull(dl3);

        DataLoader<String, String> dl4 = DataLoaderFactory.newDataLoader((keys, env) -> CompletableFuture.completedFuture(keys));
        assertNotNull(dl4);

        DataLoader<String, String> dl5 = DataLoaderFactory.newMappedDataLoader(keys -> {
            Map<String, String> map = new HashMap<>();
            for (String k : keys) map.put(k, k);
            return CompletableFuture.completedFuture(map);
        });
        assertNotNull(dl5);

        MappedBatchLoader<String, Try<String>> mappedTryLoader = keys -> {
            Map<String, Try<String>> map = new HashMap<>();
            for (String k : keys) map.put(k, Try.succeeded(k));
            return CompletableFuture.completedFuture(map);
        };
        DataLoader<String, String> dl6 = DataLoaderFactory.newMappedDataLoaderWithTry(mappedTryLoader);
        assertNotNull(dl6);

        DataLoader<String, String> dl7 = DataLoaderFactory.newPublisherDataLoader((keys, subscriber) -> {
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        });
        assertNotNull(dl7);

        DataLoader<String, String> dl8 = DataLoaderFactory.newMappedPublisherDataLoader((keys, subscriber) -> {
            for (String k : keys) subscriber.onNext(new AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        });
        assertNotNull(dl8);
    }

    @Test
    void identityBatchLoader() {
        BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").batchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void identityBatchLoaderWithContext() {
        BatchLoaderWithContext<String, String> loader = (keys, env) -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").batchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void identityMappedBatchLoader() {
        MappedBatchLoader<String, String> loader = keys -> {
            Map<String, String> map = new HashMap<>();
            for (String k : keys) map.put(k, k);
            return CompletableFuture.completedFuture(map);
        };
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newMappedDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newMappedDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").mappedBatchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void identityMappedBatchLoaderWithContext() {
        MappedBatchLoaderWithContext<String, String> loader = (keys, env) -> {
            Map<String, String> map = new HashMap<>();
            for (String k : keys) map.put(k, k);
            return CompletableFuture.completedFuture(map);
        };
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newMappedDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newMappedDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").mappedBatchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void identityBatchPublisher() {
        BatchPublisher<String, String> loader = (keys, subscriber) -> {
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newPublisherDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newPublisherDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").publisherBatchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void identityBatchPublisherWithContext() {
        BatchPublisherWithContext<String, String> loader = (keys, subscriber, env) -> {
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newPublisherDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newPublisherDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").publisherBatchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void identityMappedBatchPublisher() {
        MappedBatchPublisher<String, String> loader = (keys, subscriber) -> {
            for (String k : keys) subscriber.onNext(new AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newMappedPublisherDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newMappedPublisherDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").mappedPublisherBatchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void identityMappedBatchPublisherWithContext() {
        MappedBatchPublisherWithContext<String, String> loader = (keys, subscriber, env) -> {
            for (String k : keys) subscriber.onNext(new AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoaderOptions options = DataLoaderOptions.newOptions().build();

        DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(loader);
        assertNull(dl.getName());
        assertSame(loader, dl.getBatchLoadFunction());

        DataLoader<String, String> dl2 = DataLoaderFactory.newMappedPublisherDataLoader(loader, options);
        assertSame(options, dl2.getOptions());
        assertSame(loader, dl2.getBatchLoadFunction());

        DataLoader<String, String> dl3 = DataLoaderFactory.newMappedPublisherDataLoader("name", loader, options);
        assertThat(dl3.getName(), equalTo("name"));
        assertSame(options, dl3.getOptions());
        assertSame(loader, dl3.getBatchLoadFunction());

        DataLoader<String, String> dl4 = DataLoaderFactory.<String, String>builder()
                .name("builder-name").mappedPublisherBatchLoader(loader).options(options).build();
        assertThat(dl4.getName(), equalTo("builder-name"));
        assertSame(options, dl4.getOptions());
        assertSame(loader, dl4.getBatchLoadFunction());
    }

    @Test
    void behaviorBatchLoader_normal() throws Exception {
        BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);

        CompletableFuture<String> f1 = dl.load("A");
        CompletableFuture<String> f2 = dl.load("B");
        dl.dispatch().join();

        assertThat(f1.get(), equalTo("A"));
        assertThat(f2.get(), equalTo("B"));
    }

    @Test
    void behaviorMappedBatchLoader_missingKeyReturnsNull() throws Exception {
        MappedBatchLoader<String, String> loader = keys -> {
            Map<String, String> map = new HashMap<>();
            map.put("A", "valueA");
            return CompletableFuture.completedFuture(map);
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(loader);

        CompletableFuture<String> f1 = dl.load("A");
        CompletableFuture<String> f2 = dl.load("B");
        dl.dispatch().join();

        assertThat(f1.get(), equalTo("valueA"));
        assertNull(f2.get());
    }

    @Test
    void behaviorBatchPublisher_completesNormally() throws Exception {
        BatchPublisher<String, String> loader = (keys, subscriber) -> {
            subscriber.onSubscribe(new Subscription() {
                @Override public void request(long n) {}
                @Override public void cancel() {}
            });
            for (String k : keys) subscriber.onNext(k + "-processed");
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(loader);

        CompletableFuture<String> f1 = dl.load("A");
        CompletableFuture<String> f2 = dl.load("B");
        dl.dispatch().join();

        assertThat(f1.get(), equalTo("A-processed"));
        assertThat(f2.get(), equalTo("B-processed"));
    }

    @Test
    void behaviorMappedBatchPublisher_completesNormally() throws Exception {
        MappedBatchPublisher<String, String> loader = (keys, subscriber) -> {
            subscriber.onSubscribe(new Subscription() {
                @Override public void request(long n) {}
                @Override public void cancel() {}
            });
            for (String k : keys) subscriber.onNext(new AbstractMap.SimpleEntry<>(k, k + "-processed"));
            subscriber.onComplete();
        };
        DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(loader);

        CompletableFuture<String> f1 = dl.load("A");
        CompletableFuture<String> f2 = dl.load("B");
        dl.dispatch().join();

        assertThat(f1.get(), equalTo("A-processed"));
        assertThat(f2.get(), equalTo("B-processed"));
    }

    @Test
    void behaviorTryLoader_failedItemCompletesExceptionally() throws Exception {
        RuntimeException testEx = new RuntimeException("failed");
        BatchLoader<String, Try<String>> loader = keys -> CompletableFuture.completedFuture(Arrays.asList(
                Try.succeeded("ok"),
                Try.<String>failed(testEx)
        ));
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoaderWithTry(loader);

        CompletableFuture<String> f1 = dl.load("A");
        CompletableFuture<String> f2 = dl.load("B");
        dl.dispatch().join();

        assertThat(f1.get(), equalTo("ok"));
        ExecutionException ex = assertThrows(ExecutionException.class, f2::get);
        assertThat(ex.getCause(), sameInstance(testEx));
    }

    @Test
    @SuppressWarnings("unchecked")
    void instrumentationInteraction_orderPreserved() throws Exception {
        List<String> captures = new ArrayList<>();
        DataLoaderInstrumentation instr = new DataLoaderInstrumentation() {
            @Override
            public DataLoaderInstrumentationContext<Object> beginLoad(DataLoader<?, ?> dataLoader, Object key, Object loadContext) {
                captures.add("beginLoad_k:" + key);
                return new DataLoaderInstrumentationContext<Object>() {
                    @Override public void onDispatched() { captures.add("beginLoad_onDispatched_k:" + key); }
                    @Override public void onCompleted(Object result, Throwable t) { captures.add("beginLoad_onCompleted_k:" + key); }
                };
            }
            @Override
            public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                captures.add("beginDispatch");
                return new DataLoaderInstrumentationContext<DispatchResult<?>>() {
                    @Override public void onDispatched() { captures.add("beginDispatch_onDispatched"); }
                    @Override public void onCompleted(DispatchResult<?> result, Throwable t) { captures.add("beginDispatch_onCompleted"); }
                };
            }
            @Override
            public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
                captures.add("beginBatchLoader");
                return new DataLoaderInstrumentationContext<List<?>>() {
                    @Override public void onDispatched() { captures.add("beginBatchLoader_onDispatched"); }
                    @Override public void onCompleted(List<?> result, Throwable t) { captures.add("beginBatchLoader_onCompleted"); }
                };
            }
        };

        BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader("test-loader", loader);

        DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                .instrumentation(instr)
                .register("test-loader", dl)
                .build();

        DataLoader<Object, Object> registeredDl = (DataLoader<Object, Object>) registry.getDataLoader("test-loader");
        assertThat(registeredDl.getName(), equalTo("test-loader"));

        registeredDl.load("A");
        registeredDl.load("B");
        registeredDl.dispatch().join();

        assertThat(captures.size(), greaterThanOrEqualTo(2));
        assertTrue(captures.contains("beginDispatch"));
        assertTrue(captures.contains("beginBatchLoader"));
    }

    @Test
    void builderCopyFromExistingDataLoader_batchLoader() {
        BatchLoader<String, String> originalLoader = keys -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions originalOptions = DataLoaderOptions.newOptions().setBatchingEnabled(false).build();
        DataLoader<String, String> original = DataLoaderFactory.newDataLoader("original-name", originalLoader, originalOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertThat(copy.getName(), equalTo(original.getName()));
        assertSame(original.getBatchLoadFunction(), copy.getBatchLoadFunction());
        assertSame(original.getOptions(), copy.getOptions());

        try {
            CompletableFuture<String> f = copy.load("A");
            copy.dispatch().join();
            assertThat(f.get(), equalTo("A"));
        } catch (Exception e) {
            fail("Copy should work", e);
        }
    }

    @Test
    void builderCopyFromExistingDataLoader_mappedBatchLoader() {
        MappedBatchLoader<String, String> originalLoader = keys -> {
            Map<String, String> map = new HashMap<>();
            for (String k : keys) map.put(k, k);
            return CompletableFuture.completedFuture(map);
        };
        DataLoaderOptions originalOptions = DataLoaderOptions.newOptions().setCachingEnabled(false).build();
        DataLoader<String, String> original = DataLoaderFactory.newMappedDataLoader("original-name", originalLoader, originalOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertThat(copy.getName(), equalTo(original.getName()));
        assertSame(original.getBatchLoadFunction(), copy.getBatchLoadFunction());
        assertSame(original.getOptions(), copy.getOptions());

        try {
            CompletableFuture<String> f = copy.load("A");
            copy.dispatch().join();
            assertThat(f.get(), equalTo("A"));
        } catch (Exception e) {
            fail("Copy should work", e);
        }
    }

    @Test
    void builderCopyFromExistingDataLoader_batchPublisher() {
        BatchPublisher<String, String> originalLoader = (keys, subscriber) -> {
            subscriber.onSubscribe(new Subscription() {
                @Override public void request(long n) {}
                @Override public void cancel() {}
            });
            for (String k : keys) subscriber.onNext(k);
            subscriber.onComplete();
        };
        DataLoaderOptions originalOptions = DataLoaderOptions.newOptions().build();
        DataLoader<String, String> original = DataLoaderFactory.newPublisherDataLoader("original-name", originalLoader, originalOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertThat(copy.getName(), equalTo(original.getName()));
        assertSame(original.getBatchLoadFunction(), copy.getBatchLoadFunction());
        assertSame(original.getOptions(), copy.getOptions());

        try {
            CompletableFuture<String> f = copy.load("A");
            copy.dispatch().join();
            assertThat(f.get(), equalTo("A"));
        } catch (Exception e) {
            fail("Copy should work", e);
        }
    }

    @Test
    void builderCopyFromExistingDataLoader_mappedBatchPublisher() {
        MappedBatchPublisher<String, String> originalLoader = (keys, subscriber) -> {
            subscriber.onSubscribe(new Subscription() {
                @Override public void request(long n) {}
                @Override public void cancel() {}
            });
            for (String k : keys) subscriber.onNext(new AbstractMap.SimpleEntry<>(k, k));
            subscriber.onComplete();
        };
        DataLoaderOptions originalOptions = DataLoaderOptions.newOptions().build();
        DataLoader<String, String> original = DataLoaderFactory.newMappedPublisherDataLoader("original-name", originalLoader, originalOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertThat(copy.getName(), equalTo(original.getName()));
        assertSame(original.getBatchLoadFunction(), copy.getBatchLoadFunction());
        assertSame(original.getOptions(), copy.getOptions());

        try {
            CompletableFuture<String> f = copy.load("A");
            copy.dispatch().join();
            assertThat(f.get(), equalTo("A"));
        } catch (Exception e) {
            fail("Copy should work", e);
        }
    }

    @Test
    void nameParameterNotNullConstraint_nameOverloadThrowsWhenNull() {
        BatchLoader<Object, Object> loader = keys -> null;

        assertThrows(NullPointerException.class, () -> {
            DataLoaderFactory.newDataLoader((String) null, loader, null);
        }, "name parameter in signature should be nonNull checked");

        assertThrows(NullPointerException.class, () -> {
            DataLoaderFactory.newMappedDataLoader((String) null, (MappedBatchLoader<Object, Object>) null, null);
        });

        assertThrows(NullPointerException.class, () -> {
            DataLoaderFactory.newPublisherDataLoader((String) null, (BatchPublisher<Object, Object>) null, null);
        });

        assertThrows(NullPointerException.class, () -> {
            DataLoaderFactory.newMappedPublisherDataLoader((String) null, (MappedBatchPublisher<Object, Object>) null, null);
        });
    }

    @Test
    void nullOptionsAllowed_semanticPreserved() {
        BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);

        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader, null);
        assertNotNull(dl.getOptions());

        DataLoader<String, String> dl2 = DataLoaderFactory.newDataLoader("name", loader, null);
        assertNotNull(dl2.getOptions());

        DataLoader<String, String> dl3 = DataLoaderFactory.newMappedDataLoader(
                (MappedBatchLoader<String, String>) keys -> CompletableFuture.completedFuture(Collections.emptyMap()),
                (DataLoaderOptions) null);
        assertNotNull(dl3.getOptions());
    }
}
