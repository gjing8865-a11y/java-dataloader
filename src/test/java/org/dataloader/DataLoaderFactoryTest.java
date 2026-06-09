package org.dataloader;

import org.dataloader.instrumentation.ChainedDataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import java.lang.reflect.Method;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataLoaderFactoryTest {

    @Nested
    @DisplayName("1. Public API Reflection Snapshot Tests")
    class PublicApiReflectionTests {

        private String sig(Method m) {
            return m.getName() +
                    "(" + Arrays.stream(m.getGenericParameterTypes()).map(Type::getTypeName).collect(Collectors.joining(",")) + ")" +
                    "->" + m.getGenericReturnType().getTypeName();
        }

        @Test
        @DisplayName("All expected public static factory methods exist on DataLoaderFactory")
        void allFactoryMethodsExist() {
            Class<?> cls = DataLoaderFactory.class;
            Map<String, Method> methods = new HashMap<>();
            for (Method m : cls.getDeclaredMethods()) {
                if (java.lang.reflect.Modifier.isPublic(m.getModifiers()) && java.lang.reflect.Modifier.isStatic(m.getModifiers())) {
                    methods.put(sig(m), m);
                }
            }

            assertMethodPresent(methods, "newDataLoader", BatchLoader.class);
            assertMethodPresent(methods, "newDataLoader", String.class, BatchLoader.class);
            assertMethodPresent(methods, "newDataLoader", BatchLoader.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newDataLoader", String.class, BatchLoader.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newDataLoaderWithTry", BatchLoader.class);
            assertMethodPresent(methods, "newDataLoaderWithTry", BatchLoader.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newDataLoaderWithTry", String.class, BatchLoader.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newDataLoader", BatchLoaderWithContext.class);
            assertMethodPresent(methods, "newDataLoader", BatchLoaderWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newDataLoader", String.class, BatchLoaderWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newDataLoaderWithTry", BatchLoaderWithContext.class);
            assertMethodPresent(methods, "newDataLoaderWithTry", BatchLoaderWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newDataLoaderWithTry", String.class, BatchLoaderWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedDataLoader", MappedBatchLoader.class);
            assertMethodPresent(methods, "newMappedDataLoader", MappedBatchLoader.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedDataLoader", String.class, MappedBatchLoader.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedDataLoaderWithTry", MappedBatchLoader.class);
            assertMethodPresent(methods, "newMappedDataLoaderWithTry", MappedBatchLoader.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedDataLoaderWithTry", String.class, MappedBatchLoader.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedDataLoader", MappedBatchLoaderWithContext.class);
            assertMethodPresent(methods, "newMappedDataLoader", MappedBatchLoaderWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedDataLoader", String.class, MappedBatchLoaderWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedDataLoaderWithTry", MappedBatchLoaderWithContext.class);
            assertMethodPresent(methods, "newMappedDataLoaderWithTry", MappedBatchLoaderWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedDataLoaderWithTry", String.class, MappedBatchLoaderWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newPublisherDataLoader", BatchPublisher.class);
            assertMethodPresent(methods, "newPublisherDataLoader", BatchPublisher.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newPublisherDataLoader", String.class, BatchPublisher.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newPublisherDataLoaderWithTry", BatchPublisher.class);
            assertMethodPresent(methods, "newPublisherDataLoaderWithTry", BatchPublisher.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newPublisherDataLoaderWithTry", String.class, BatchPublisher.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newPublisherDataLoader", BatchPublisherWithContext.class);
            assertMethodPresent(methods, "newPublisherDataLoader", BatchPublisherWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newPublisherDataLoader", String.class, BatchPublisherWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newPublisherDataLoaderWithTry", BatchPublisherWithContext.class);
            assertMethodPresent(methods, "newPublisherDataLoaderWithTry", BatchPublisherWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newPublisherDataLoaderWithTry", String.class, BatchPublisherWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedPublisherDataLoader", MappedBatchPublisher.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoader", MappedBatchPublisher.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoader", String.class, MappedBatchPublisher.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedPublisherDataLoaderWithTry", MappedBatchPublisher.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoaderWithTry", MappedBatchPublisher.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoaderWithTry", String.class, MappedBatchPublisher.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedPublisherDataLoader", MappedBatchPublisherWithContext.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoader", MappedBatchPublisherWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoader", String.class, MappedBatchPublisherWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "newMappedPublisherDataLoaderWithTry", MappedBatchPublisherWithContext.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoaderWithTry", MappedBatchPublisherWithContext.class, DataLoaderOptions.class);
            assertMethodPresent(methods, "newMappedPublisherDataLoaderWithTry", String.class, MappedBatchPublisherWithContext.class, DataLoaderOptions.class);

            assertMethodPresent(methods, "builder");
            assertMethodPresent(methods, "builder", DataLoader.class);
        }

        private void assertMethodPresent(Map<String, Method> methods, String name, Class<?>... paramTypes) {
            boolean found = methods.keySet().stream().anyMatch(s -> {
                if (!s.startsWith(name + "(")) return false;
                if (paramTypes.length == 0) return s.equals(name + "()->" + methods.get(s).getGenericReturnType().getTypeName());
                for (Class<?> pt : paramTypes) {
                    if (!s.contains(pt.getName())) return false;
                }
                return true;
            });
            assertTrue(found, "Missing public static method: " + name + Arrays.toString(paramTypes));
        }

        @Test
        @DisplayName("All factory methods return DataLoader<K,V>")
        void allFactoryMethodsReturnDataLoader() {
            for (Method m : DataLoaderFactory.class.getDeclaredMethods()) {
                if (java.lang.reflect.Modifier.isPublic(m.getModifiers()) && java.lang.reflect.Modifier.isStatic(m.getModifiers())) {
                    if (m.getName().equals("builder")) continue;
                    assertThat("Method " + m.getName() + " should return DataLoader",
                            DataLoader.class.isAssignableFrom(m.getReturnType()), equalTo(true));
                }
            }
        }

        @Test
        @DisplayName("Builder public methods contain all expected entries")
        void builderPublicMethodsExist() {
            Class<?> builderCls = DataLoaderFactory.Builder.class;
            Set<String> methodNames = Arrays.stream(builderCls.getDeclaredMethods())
                    .filter(m -> java.lang.reflect.Modifier.isPublic(m.getModifiers()))
                    .map(Method::getName)
                    .collect(Collectors.toSet());

            assertThat(methodNames, notNullValue());
            assertTrue(methodNames.contains("name"), "Builder must have 'name' method");
            assertTrue(methodNames.contains("options"), "Builder must have 'options' method");
            assertTrue(methodNames.contains("batchLoadFunction"), "Builder must have 'batchLoadFunction' method");
            assertTrue(methodNames.contains("batchLoader"), "Builder must have 'batchLoader' method");
            assertTrue(methodNames.contains("mappedBatchLoader"), "Builder must have 'mappedBatchLoader' method");
            assertTrue(methodNames.contains("publisherBatchLoader"), "Builder must have 'publisherBatchLoader' method");
            assertTrue(methodNames.contains("mappedPublisherBatchLoader"), "Builder must have 'mappedPublisherBatchLoader' method");
            assertTrue(methodNames.contains("build"), "Builder must have 'build' method");
        }

        @Test
        @DisplayName("No new public methods introduced on DataLoaderFactory")
        void noNewPublicMethods() {
            Set<String> allowedNames = Set.of(
                    "newDataLoader", "newDataLoaderWithTry",
                    "newMappedDataLoader", "newMappedDataLoaderWithTry",
                    "newPublisherDataLoader", "newPublisherDataLoaderWithTry",
                    "newMappedPublisherDataLoader", "newMappedPublisherDataLoaderWithTry",
                    "builder"
            );
            for (Method m : DataLoaderFactory.class.getDeclaredMethods()) {
                if (java.lang.reflect.Modifier.isPublic(m.getModifiers()) && java.lang.reflect.Modifier.isStatic(m.getModifiers())) {
                    assertTrue(allowedNames.contains(m.getName()),
                            "Unexpected public static method: " + m.getName());
                }
            }
        }
    }

    @Nested
    @DisplayName("2. Source-Level Generic Inference Tests")
    class GenericInferenceTests {

        @Test
        @DisplayName("BatchLoader lambda infers K,V without raw type or cast")
        void batchLoaderInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newDataLoader(
                    (List<String> keys) -> CompletableFuture.completedFuture(
                            keys.stream().map(String::length).collect(Collectors.toList())
                    )
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("BatchLoaderWithContext lambda infers K,V")
        void batchLoaderWithContextInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newDataLoader(
                    (List<String> keys, BatchLoaderEnvironment env) -> CompletableFuture.completedFuture(
                            keys.stream().map(String::length).collect(Collectors.toList())
                    )
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("MappedBatchLoader lambda infers K,V")
        void mappedBatchLoaderInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newMappedDataLoader(
                    (Set<String> keys) -> {
                        Map<String, Integer> map = new HashMap<>();
                        keys.forEach(k -> map.put(k, k.length()));
                        return CompletableFuture.completedFuture(map);
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("MappedBatchLoaderWithContext lambda infers K,V")
        void mappedBatchLoaderWithContextInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newMappedDataLoader(
                    (Set<String> keys, BatchLoaderEnvironment env) -> {
                        Map<String, Integer> map = new HashMap<>();
                        keys.forEach(k -> map.put(k, k.length()));
                        return CompletableFuture.completedFuture(map);
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("BatchPublisher lambda infers K,V")
        void batchPublisherInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newPublisherDataLoader(
                    (List<String> keys, Subscriber<Integer> subscriber) -> {
                        keys.forEach(k -> subscriber.onNext(k.length()));
                        subscriber.onComplete();
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("BatchPublisherWithContext lambda infers K,V")
        void batchPublisherWithContextInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newPublisherDataLoader(
                    (List<String> keys, Subscriber<Integer> subscriber, BatchLoaderEnvironment env) -> {
                        keys.forEach(k -> subscriber.onNext(k.length()));
                        subscriber.onComplete();
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("MappedBatchPublisher lambda infers K,V")
        void mappedBatchPublisherInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newMappedPublisherDataLoader(
                    (Set<String> keys, Subscriber<Map.Entry<String, Integer>> subscriber) -> {
                        keys.forEach(k -> subscriber.onNext(new Map.Entry<String, Integer>() {
                            public String getKey() { return k; }
                            public Integer getValue() { return k.length(); }
                            public Integer setValue(Integer value) { return value; }
                        }));
                        subscriber.onComplete();
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("MappedBatchPublisherWithContext lambda infers K,V")
        void mappedBatchPublisherWithContextInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newMappedPublisherDataLoader(
                    (List<String> keys, Subscriber<Map.Entry<String, Integer>> subscriber, BatchLoaderEnvironment env) -> {
                        keys.forEach(k -> subscriber.onNext(new Map.Entry<String, Integer>() {
                            public String getKey() { return k; }
                            public Integer getValue() { return k.length(); }
                            public Integer setValue(Integer value) { return value; }
                        }));
                        subscriber.onComplete();
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("newDataLoaderWithTry infers K,V via lambda")
        void tryBatchLoaderInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newDataLoaderWithTry(
                    (List<String> keys) -> CompletableFuture.completedFuture(
                            keys.stream().map(k -> Try.succeeded(k.length())).collect(Collectors.toList())
                    )
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("newMappedDataLoaderWithTry infers K,V via lambda")
        void tryMappedBatchLoaderInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newMappedDataLoaderWithTry(
                    (Set<String> keys) -> {
                        Map<String, Try<Integer>> map = new HashMap<>();
                        keys.forEach(k -> map.put(k, Try.succeeded(k.length())));
                        return CompletableFuture.completedFuture(map);
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("newPublisherDataLoaderWithTry infers K,V via lambda")
        void tryPublisherBatchLoaderInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newPublisherDataLoaderWithTry(
                    (List<String> keys, Subscriber<Try<Integer>> subscriber) -> {
                        keys.forEach(k -> subscriber.onNext(Try.succeeded(k.length())));
                        subscriber.onComplete();
                    }
            );
            assertNotNull(dl);
        }

        @Test
        @DisplayName("newMappedPublisherDataLoaderWithTry infers K,V via lambda")
        void tryMappedPublisherBatchLoaderInference() {
            DataLoader<String, Integer> dl = DataLoaderFactory.newMappedPublisherDataLoaderWithTry(
                    (Set<String> keys, Subscriber<Map.Entry<String, Try<Integer>>> subscriber) -> {
                        keys.forEach(k -> subscriber.onNext(new Map.Entry<String, Try<Integer>>() {
                            public String getKey() { return k; }
                            public Try<Integer> getValue() { return Try.succeeded(k.length()); }
                            public Try<Integer> setValue(Try<Integer> value) { return value; }
                        }));
                        subscriber.onComplete();
                    }
            );
            assertNotNull(dl);
        }
    }

    @Nested
    @DisplayName("3. Identity Test Matrix")
    class IdentityTests {

        @Test
        @DisplayName("BatchLoader: no-name + null options")
        void batchLoader_noName_nullOptions() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), notNullValue());
        }

        @Test
        @DisplayName("BatchLoader: name + explicit options")
        void batchLoader_name_options() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader("test", loader, opts);
            assertThat(dl.getName(), equalTo("test"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("BatchLoader: Builder path")
        void batchLoader_builder() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.<String, String>builder()
                    .name("b").batchLoader(loader).options(opts).build();
            assertThat(dl.getName(), equalTo("b"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("BatchLoaderWithContext: no-name + null options")
        void batchLoaderWithContext_noName_nullOptions() {
            BatchLoaderWithContext<String, String> loader = (keys, env) -> CompletableFuture.completedFuture(keys);
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
        }

        @Test
        @DisplayName("BatchLoaderWithContext: name + explicit options")
        void batchLoaderWithContext_name_options() {
            BatchLoaderWithContext<String, String> loader = (keys, env) -> CompletableFuture.completedFuture(keys);
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader("ctx", loader, opts);
            assertThat(dl.getName(), equalTo("ctx"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("MappedBatchLoader: no-name + null options")
        void mappedBatchLoader_noName_nullOptions() {
            MappedBatchLoader<String, String> loader = keys -> {
                Map<String, String> map = new HashMap<>();
                keys.forEach(k -> map.put(k, k));
                return CompletableFuture.completedFuture(map);
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
        }

        @Test
        @DisplayName("MappedBatchLoader: name + explicit options")
        void mappedBatchLoader_name_options() {
            MappedBatchLoader<String, String> loader = keys -> {
                Map<String, String> map = new HashMap<>();
                keys.forEach(k -> map.put(k, k));
                return CompletableFuture.completedFuture(map);
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader("m", loader, opts);
            assertThat(dl.getName(), equalTo("m"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("MappedBatchLoaderWithContext: no-name + null options")
        void mappedBatchLoaderWithContext_noName_nullOptions() {
            MappedBatchLoaderWithContext<String, String> loader = (keys, env) -> {
                Map<String, String> map = new HashMap<>();
                keys.forEach(k -> map.put(k, k));
                return CompletableFuture.completedFuture(map);
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
        }

        @Test
        @DisplayName("MappedBatchLoaderWithContext: name + explicit options")
        void mappedBatchLoaderWithContext_name_options() {
            MappedBatchLoaderWithContext<String, String> loader = (keys, env) -> {
                Map<String, String> map = new HashMap<>();
                keys.forEach(k -> map.put(k, k));
                return CompletableFuture.completedFuture(map);
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader("mc", loader, opts);
            assertThat(dl.getName(), equalTo("mc"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("BatchPublisher: no-name + null options")
        void batchPublisher_noName_nullOptions() {
            BatchPublisher<String, String> loader = (keys, sub) -> {
                keys.forEach(sub::onNext);
                sub.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
        }

        @Test
        @DisplayName("BatchPublisher: name + explicit options")
        void batchPublisher_name_options() {
            BatchPublisher<String, String> loader = (keys, sub) -> {
                keys.forEach(sub::onNext);
                sub.onComplete();
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader("p", loader, opts);
            assertThat(dl.getName(), equalTo("p"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("BatchPublisherWithContext: no-name + null options")
        void batchPublisherWithContext_noName_nullOptions() {
            BatchPublisherWithContext<String, String> loader = (keys, sub, env) -> {
                keys.forEach(sub::onNext);
                sub.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
        }

        @Test
        @DisplayName("BatchPublisherWithContext: name + explicit options")
        void batchPublisherWithContext_name_options() {
            BatchPublisherWithContext<String, String> loader = (keys, sub, env) -> {
                keys.forEach(sub::onNext);
                sub.onComplete();
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader("pc", loader, opts);
            assertThat(dl.getName(), equalTo("pc"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("MappedBatchPublisher: no-name + null options")
        void mappedBatchPublisher_noName_nullOptions() {
            MappedBatchPublisher<String, String> loader = (keys, sub) -> {
                keys.forEach(k -> sub.onNext(new Map.Entry<String, String>() {
                    public String getKey() { return k; }
                    public String getValue() { return k; }
                    public String setValue(String v) { return v; }
                }));
                sub.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
        }

        @Test
        @DisplayName("MappedBatchPublisher: name + explicit options")
        void mappedBatchPublisher_name_options() {
            MappedBatchPublisher<String, String> loader = (keys, sub) -> {
                keys.forEach(k -> sub.onNext(new Map.Entry<String, String>() {
                    public String getKey() { return k; }
                    public String getValue() { return k; }
                    public String setValue(String v) { return v; }
                }));
                sub.onComplete();
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader("mp", loader, opts);
            assertThat(dl.getName(), equalTo("mp"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("MappedBatchPublisherWithContext: no-name + null options")
        void mappedBatchPublisherWithContext_noName_nullOptions() {
            MappedBatchPublisherWithContext<String, String> loader = (keys, sub, env) -> {
                keys.forEach(k -> sub.onNext(new Map.Entry<String, String>() {
                    public String getKey() { return k; }
                    public String getValue() { return k; }
                    public String setValue(String v) { return v; }
                }));
                sub.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(loader);
            assertThat(dl.getName(), nullValue());
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
        }

        @Test
        @DisplayName("MappedBatchPublisherWithContext: name + explicit options")
        void mappedBatchPublisherWithContext_name_options() {
            MappedBatchPublisherWithContext<String, String> loader = (keys, sub, env) -> {
                keys.forEach(k -> sub.onNext(new Map.Entry<String, String>() {
                    public String getKey() { return k; }
                    public String getValue() { return k; }
                    public String setValue(String v) { return v; }
                }));
                sub.onComplete();
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader("mpc", loader, opts);
            assertThat(dl.getName(), equalTo("mpc"));
            assertThat(dl.getBatchLoadFunction(), sameInstance(loader));
            assertThat(dl.getOptions(), sameInstance(opts));
        }

        @Test
        @DisplayName("Name overloads must validate nonNull name")
        void nameOverloadValidatesNonNull() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
            assertThrows(NullPointerException.class, () ->
                    DataLoaderFactory.newDataLoader((String) null, loader));
            assertThrows(NullPointerException.class, () ->
                    DataLoaderFactory.newDataLoader((String) null, loader, DataLoaderOptions.newOptions().build()));
        }

        @Test
        @DisplayName("No-name overloads must not add name validation")
        void noNameOverloadsNoValidation() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(keys);
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);
            assertThat(dl.getName(), nullValue());
        }
    }

    @Nested
    @DisplayName("4. Runtime Behavior Tests")
    class RuntimeBehaviorTests {

        @Test
        @DisplayName("BatchLoader load + dispatch")
        void batchLoader_loadAndDispatch() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(new ArrayList<>(keys));
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);
            CompletableFuture<String> f = dl.load("A");
            dl.dispatch();
            assertThat(f.join(), equalTo("A"));
        }

        @Test
        @DisplayName("BatchLoaderWithContext load + dispatch")
        void batchLoaderWithContext_loadAndDispatch() {
            BatchLoaderWithContext<String, String> loader = (keys, env) -> CompletableFuture.completedFuture(new ArrayList<>(keys));
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader(loader);
            CompletableFuture<String> f = dl.load("B");
            dl.dispatch();
            assertThat(f.join(), equalTo("B"));
        }

        @Test
        @DisplayName("MappedBatchLoader load + dispatch, missing key returns null")
        void mappedBatchLoader_missingKeyReturnsNull() {
            MappedBatchLoader<String, String> loader = keys -> {
                Map<String, String> map = new HashMap<>();
                keys.forEach(k -> {
                    if (!k.equals("missing")) map.put(k, k + "!");
                });
                return CompletableFuture.completedFuture(map);
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(loader);
            CompletableFuture<String> f1 = dl.load("present");
            CompletableFuture<String> f2 = dl.load("missing");
            dl.dispatch();
            assertThat(f1.join(), equalTo("present!"));
            assertThat(f2.join(), nullValue());
        }

        @Test
        @DisplayName("MappedBatchLoaderWithContext load + dispatch")
        void mappedBatchLoaderWithContext_loadAndDispatch() {
            MappedBatchLoaderWithContext<String, String> loader = (keys, env) -> {
                Map<String, String> map = new HashMap<>();
                keys.forEach(k -> map.put(k, k));
                return CompletableFuture.completedFuture(map);
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoader(loader);
            CompletableFuture<String> f = dl.load("C");
            dl.dispatch();
            assertThat(f.join(), equalTo("C"));
        }

        @Test
        @DisplayName("BatchPublisher load + dispatch completes future")
        void batchPublisher_loadAndDispatch() {
            BatchPublisher<String, String> loader = (keys, subscriber) -> {
                keys.forEach(subscriber::onNext);
                subscriber.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(loader);
            CompletableFuture<String> f = dl.load("D");
            dl.dispatch();
            assertThat(f.join(), equalTo("D"));
        }

        @Test
        @DisplayName("BatchPublisherWithContext load + dispatch")
        void batchPublisherWithContext_loadAndDispatch() {
            BatchPublisherWithContext<String, String> loader = (keys, subscriber, env) -> {
                keys.forEach(subscriber::onNext);
                subscriber.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoader(loader);
            CompletableFuture<String> f = dl.load("E");
            dl.dispatch();
            assertThat(f.join(), equalTo("E"));
        }

        @Test
        @DisplayName("MappedBatchPublisher load + dispatch")
        void mappedBatchPublisher_loadAndDispatch() {
            MappedBatchPublisher<String, String> loader = (keys, subscriber) -> {
                keys.forEach(k -> subscriber.onNext(new Map.Entry<String, String>() {
                    public String getKey() { return k; }
                    public String getValue() { return k; }
                    public String setValue(String v) { return v; }
                }));
                subscriber.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(loader);
            CompletableFuture<String> f = dl.load("F");
            dl.dispatch();
            assertThat(f.join(), equalTo("F"));
        }

        @Test
        @DisplayName("MappedBatchPublisherWithContext load + dispatch")
        void mappedBatchPublisherWithContext_loadAndDispatch() {
            MappedBatchPublisherWithContext<String, String> loader = (keys, subscriber, env) -> {
                keys.forEach(k -> subscriber.onNext(new Map.Entry<String, String>() {
                    public String getKey() { return k; }
                    public String getValue() { return k; }
                    public String setValue(String v) { return v; }
                }));
                subscriber.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoader(loader);
            CompletableFuture<String> f = dl.load("G");
            dl.dispatch();
            assertThat(f.join(), equalTo("G"));
        }

        @Test
        @DisplayName("Try BatchLoader: failed item causes load future to complete exceptionally")
        void tryBatchLoader_failure() {
            RuntimeException err = new RuntimeException("boom");
            BatchLoader<String, Try<String>> loader = keys -> CompletableFuture.completedFuture(
                    keys.stream().map(k -> k.equals("bad") ? Try.<String>failed(err) : Try.succeeded(k))
                            .collect(Collectors.toList())
            );
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoaderWithTry(loader);
            CompletableFuture<String> fGood = dl.load("good");
            CompletableFuture<String> fBad = dl.load("bad");
            dl.dispatch();
            assertThat(fGood.join(), equalTo("good"));
            assertTrue(fBad.isCompletedExceptionally());
        }

        @Test
        @DisplayName("Try MappedBatchLoader: failed item causes load future to complete exceptionally")
        void tryMappedBatchLoader_failure() {
            RuntimeException err = new RuntimeException("boom");
            MappedBatchLoader<String, Try<String>> loader = keys -> {
                Map<String, Try<String>> map = new HashMap<>();
                keys.forEach(k -> map.put(k, k.equals("bad") ? Try.failed(err) : Try.succeeded(k)));
                return CompletableFuture.completedFuture(map);
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedDataLoaderWithTry(loader);
            CompletableFuture<String> fGood = dl.load("good");
            CompletableFuture<String> fBad = dl.load("bad");
            dl.dispatch();
            assertThat(fGood.join(), equalTo("good"));
            assertTrue(fBad.isCompletedExceptionally());
        }

        @Test
        @DisplayName("Try BatchPublisher: failed item causes load future to complete exceptionally")
        void tryPublisher_failure() {
            RuntimeException err = new RuntimeException("boom");
            BatchPublisher<String, Try<String>> loader = (keys, subscriber) -> {
                keys.forEach(k -> subscriber.onNext(k.equals("bad") ? Try.failed(err) : Try.succeeded(k)));
                subscriber.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newPublisherDataLoaderWithTry(loader);
            CompletableFuture<String> fGood = dl.load("good");
            CompletableFuture<String> fBad = dl.load("bad");
            dl.dispatch();
            assertThat(fGood.join(), equalTo("good"));
            assertTrue(fBad.isCompletedExceptionally());
        }

        @Test
        @DisplayName("Try MappedBatchPublisher: failed item causes load future to complete exceptionally")
        void tryMappedPublisher_failure() {
            RuntimeException err = new RuntimeException("boom");
            MappedBatchPublisher<String, Try<String>> loader = (keys, subscriber) -> {
                keys.forEach(k -> subscriber.onNext(new Map.Entry<String, Try<String>>() {
                    public String getKey() { return k; }
                    public Try<String> getValue() { return k.equals("bad") ? Try.failed(err) : Try.succeeded(k); }
                    public Try<String> setValue(Try<String> v) { return v; }
                }));
                subscriber.onComplete();
            };
            DataLoader<String, String> dl = DataLoaderFactory.newMappedPublisherDataLoaderWithTry(loader);
            CompletableFuture<String> fGood = dl.load("good");
            CompletableFuture<String> fBad = dl.load("bad");
            dl.dispatch();
            assertThat(fGood.join(), equalTo("good"));
            assertTrue(fBad.isCompletedExceptionally());
        }
    }

    @Nested
    @DisplayName("5. Instrumentation Interaction Tests")
    class InstrumentationTests {

        @Test
        @DisplayName("Factory-created loader registered in registry gets instrumentation injected")
        void registryInstrumentationInjection() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(new ArrayList<>(keys));
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader("instrTest", loader);

            DataLoaderInstrumentation simpleInstr = new DataLoaderInstrumentation() {};
            ChainedDataLoaderInstrumentation chainedInstr = new ChainedDataLoaderInstrumentation().add(simpleInstr);

            DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                    .instrumentation(chainedInstr)
                    .register("instrTest", dl)
                    .build();

            DataLoader<String, String> registeredDl = registry.getDataLoader("instrTest");
            assertNotNull(registeredDl);

            DataLoaderInstrumentation instr = registeredDl.getOptions().getInstrumentation();
            assertThat(instr, instanceOf(ChainedDataLoaderInstrumentation.class));
        }

        @Test
        @DisplayName("beginDispatch / beginBatchLoader callback order is preserved")
        void callbackOrderPreserved() {
            List<String> callbackNames = new CopyOnWriteArrayList<>();
            DataLoaderInstrumentation trackingInstr = new DataLoaderInstrumentation() {
                @Override
                public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                    callbackNames.add("beginDispatch");
                    return null;
                }

                @Override
                public DataLoaderInstrumentationContext<List<?>> beginBatchLoader(DataLoader<?, ?> dataLoader, List<?> keys, BatchLoaderEnvironment environment) {
                    callbackNames.add("beginBatchLoader");
                    return null;
                }
            };
            ChainedDataLoaderInstrumentation chainedInstr = new ChainedDataLoaderInstrumentation().add(trackingInstr);

            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(new ArrayList<>(keys));
            DataLoaderOptions opts = DataLoaderOptions.newOptions().setInstrumentation(chainedInstr).build();
            DataLoader<String, String> dl = DataLoaderFactory.newDataLoader("orderTest", loader, opts);

            dl.load("A");
            dl.dispatch();

            assertTrue(callbackNames.size() >= 2, "Expected at least beginDispatch and beginBatchLoader callbacks");
            assertThat(callbackNames.get(0), equalTo("beginDispatch"));
        }
    }

    @Nested
    @DisplayName("6. Builder(existingDataLoader) Regression Tests")
    class BuilderCopyTests {

        @Test
        @DisplayName("builder(existing) copies BatchLoader name/options/function identity")
        void builderCopy_batchLoader() {
            BatchLoader<String, String> loader = keys -> CompletableFuture.completedFuture(new ArrayList<>(keys));
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> original = DataLoaderFactory.newDataLoader("copyBL", loader, opts);

            DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();
            assertThat(copy.getName(), equalTo("copyBL"));
            assertThat(copy.getBatchLoadFunction(), sameInstance(loader));
            assertThat(copy.getOptions(), sameInstance(opts));

            CompletableFuture<String> f = copy.load("X");
            copy.dispatch();
            assertThat(f.join(), equalTo("X"));
        }

        @Test
        @DisplayName("builder(existing) copies MappedBatchLoader name/options/function identity")
        void builderCopy_mappedBatchLoader() {
            MappedBatchLoader<String, String> loader = keys -> {
                Map<String, String> map = new HashMap<>();
                keys.forEach(k -> map.put(k, k));
                return CompletableFuture.completedFuture(map);
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> original = DataLoaderFactory.newMappedDataLoader("copyMBL", loader, opts);

            DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();
            assertThat(copy.getName(), equalTo("copyMBL"));
            assertThat(copy.getBatchLoadFunction(), sameInstance(loader));
            assertThat(copy.getOptions(), sameInstance(opts));

            CompletableFuture<String> f = copy.load("Y");
            copy.dispatch();
            assertThat(f.join(), equalTo("Y"));
        }

        @Test
        @DisplayName("builder(existing) copies BatchPublisher name/options/function identity")
        void builderCopy_batchPublisher() {
            BatchPublisher<String, String> loader = (keys, sub) -> {
                keys.forEach(sub::onNext);
                sub.onComplete();
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> original = DataLoaderFactory.newPublisherDataLoader("copyBP", loader, opts);

            DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();
            assertThat(copy.getName(), equalTo("copyBP"));
            assertThat(copy.getBatchLoadFunction(), sameInstance(loader));
            assertThat(copy.getOptions(), sameInstance(opts));

            CompletableFuture<String> f = copy.load("Z");
            copy.dispatch();
            assertThat(f.join(), equalTo("Z"));
        }

        @Test
        @DisplayName("builder(existing) copies MappedBatchPublisher name/options/function identity")
        void builderCopy_mappedBatchPublisher() {
            MappedBatchPublisher<String, String> loader = (keys, sub) -> {
                keys.forEach(k -> sub.onNext(new Map.Entry<String, String>() {
                    public String getKey() { return k; }
                    public String getValue() { return k; }
                    public String setValue(String v) { return v; }
                }));
                sub.onComplete();
            };
            DataLoaderOptions opts = DataLoaderOptions.newOptions().build();
            DataLoader<String, String> original = DataLoaderFactory.newMappedPublisherDataLoader("copyMBP", loader, opts);

            DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();
            assertThat(copy.getName(), equalTo("copyMBP"));
            assertThat(copy.getBatchLoadFunction(), sameInstance(loader));
            assertThat(copy.getOptions(), sameInstance(opts));

            CompletableFuture<String> f = copy.load("W");
            copy.dispatch();
            assertThat(f.join(), equalTo("W"));
        }
    }

    @Nested
    @DisplayName("Original test compatibility")
    class OriginalTests {

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
    }
}
