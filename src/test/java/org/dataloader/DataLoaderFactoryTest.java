package org.dataloader;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
    void public_static_factory_methods_keep_api_snapshot() {
        List<String> descriptors = publicStaticMethodDescriptors(DataLoaderFactory.class);

        assertThat(descriptors.size(), equalTo(51));
        assertHasPublicStaticMethod(DataLoaderFactory.class, "builder");
        assertHasPublicStaticMethod(DataLoaderFactory.class, "builder", DataLoader.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newDataLoader", BatchLoader.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newDataLoaderWithTry", BatchLoader.class, DataLoaderOptions.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newMappedDataLoader", MappedBatchLoader.class, DataLoaderOptions.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newMappedDataLoaderWithTry", String.class, MappedBatchLoader.class, DataLoaderOptions.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newPublisherDataLoader", BatchPublisher.class, DataLoaderOptions.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newPublisherDataLoaderWithTry", String.class, BatchPublisherWithContext.class, DataLoaderOptions.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newMappedPublisherDataLoader", MappedBatchPublisherWithContext.class, DataLoaderOptions.class);
        assertHasPublicStaticMethod(DataLoaderFactory.class, "newMappedPublisherDataLoaderWithTry", String.class, MappedBatchPublisher.class, DataLoaderOptions.class);
    }

    @Test
    void builder_public_methods_keep_api_snapshot() {
        List<String> descriptors = publicDeclaredMethodDescriptors(DataLoaderFactory.Builder.class);

        assertThat(descriptors.size(), equalTo(12));
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "name", String.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "options", DataLoaderOptions.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "batchLoadFunction", Object.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "batchLoader", BatchLoader.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "batchLoader", BatchLoaderWithContext.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "mappedBatchLoader", MappedBatchLoader.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "mappedBatchLoader", MappedBatchLoaderWithContext.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "publisherBatchLoader", BatchPublisher.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "publisherBatchLoader", BatchPublisherWithContext.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "mappedPublisherBatchLoader", MappedBatchPublisher.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "mappedPublisherBatchLoader", MappedBatchPublisherWithContext.class);
        assertHasPublicMethod(DataLoaderFactory.Builder.class, "build");
    }

    @Test
    void source_level_generic_inference_still_works_for_factory_overloads() {
        DataLoader<String, Integer> batchLoader = DataLoaderFactory.newDataLoader(keys -> CompletableFuture.completedFuture(keys.stream().map(String::length).collect(toList())));
        DataLoader<String, Integer> batchLoaderWithContext = DataLoaderFactory.newDataLoader((keys, environment) -> CompletableFuture.completedFuture(keys.stream().map(String::length).collect(toList())));
        DataLoader<String, Integer> mappedBatchLoader = DataLoaderFactory.newMappedDataLoader(keys -> CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(Function.identity(), String::length))));
        DataLoader<String, Integer> mappedBatchLoaderWithContext = DataLoaderFactory.newMappedDataLoader((keys, environment) -> CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(Function.identity(), String::length))));
        DataLoader<String, Integer> publisherLoader = DataLoaderFactory.newPublisherDataLoader((keys, subscriber) -> Flux.fromIterable(keys.stream().map(String::length).collect(toList())).subscribe(subscriber));
        DataLoader<String, Integer> publisherLoaderWithContext = DataLoaderFactory.newPublisherDataLoader((keys, subscriber, environment) -> Flux.fromIterable(keys.stream().map(String::length).collect(toList())).subscribe(subscriber));
        DataLoader<String, Integer> mappedPublisherLoader = DataLoaderFactory.newMappedPublisherDataLoader((keys, subscriber) -> Flux.fromIterable(keys.stream().map(key -> Map.entry(key, key.length())).collect(toList())).subscribe(subscriber));
        DataLoader<String, Integer> mappedPublisherLoaderWithContext = DataLoaderFactory.newMappedPublisherDataLoader((keys, subscriber, environment) -> Flux.fromIterable(keys.stream().map(key -> Map.entry(key, key.length())).collect(toList())).subscribe(subscriber));

        DataLoader<String, Integer> batchTryLoader = DataLoaderFactory.newDataLoaderWithTry(keys -> CompletableFuture.completedFuture(keys.stream().map(key -> key.startsWith("!") ? Try.<Integer>failed(new IllegalStateException(key)) : Try.succeeded(key.length())).collect(toList())));
        DataLoader<String, Integer> batchTryLoaderWithContext = DataLoaderFactory.newDataLoaderWithTry((keys, environment) -> CompletableFuture.completedFuture(keys.stream().map(key -> key.startsWith("!") ? Try.<Integer>failed(new IllegalStateException(key)) : Try.succeeded(key.length())).collect(toList())));
        DataLoader<String, Integer> mappedTryLoader = DataLoaderFactory.newMappedDataLoaderWithTry(keys -> CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(Function.identity(), key -> key.startsWith("!") ? Try.<Integer>failed(new IllegalStateException(key)) : Try.succeeded(key.length())))));
        DataLoader<String, Integer> mappedPublisherTryLoader = DataLoaderFactory.newMappedPublisherDataLoaderWithTry((keys, subscriber) -> Flux.fromIterable(keys.stream().map(key -> Map.entry(key, key.startsWith("!") ? Try.<Integer>failed(new IllegalStateException(key)) : Try.succeeded(key.length()))).collect(toList())).subscribe(subscriber));

        assertThat(List.of(
                batchLoader,
                batchLoaderWithContext,
                mappedBatchLoader,
                mappedBatchLoaderWithContext,
                publisherLoader,
                publisherLoaderWithContext,
                mappedPublisherLoader,
                mappedPublisherLoaderWithContext,
                batchTryLoader,
                batchTryLoaderWithContext,
                mappedTryLoader,
                mappedPublisherTryLoader
        ).stream().allMatch(Objects::nonNull), equalTo(true));
    }

    @Test
    void factory_variants_preserve_name_options_and_batch_load_function_identity() {
        DataLoaderOptions explicitOptions = DataLoaderOptions.newOptions().setCachingEnabled(false).build();

        List<IdentityVariant<?>> variants = List.of(
                new IdentityVariant<>(
                        "BatchLoader",
                        (BatchLoader<String, String>) keys -> CompletableFuture.completedFuture(new ArrayList<>(keys)),
                        (factory, options) -> DataLoaderFactory.newDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().batchLoader(factory)
                ),
                new IdentityVariant<>(
                        "BatchLoaderWithContext",
                        (BatchLoaderWithContext<String, String>) (keys, environment) -> CompletableFuture.completedFuture(new ArrayList<>(keys)),
                        (factory, options) -> DataLoaderFactory.newDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().batchLoader(factory)
                ),
                new IdentityVariant<>(
                        "MappedBatchLoader",
                        (MappedBatchLoader<String, String>) keys -> CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(Function.identity(), Function.identity(), (left, right) -> right, LinkedHashMap::new))),
                        (factory, options) -> DataLoaderFactory.newMappedDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newMappedDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().mappedBatchLoader(factory)
                ),
                new IdentityVariant<>(
                        "MappedBatchLoaderWithContext",
                        (MappedBatchLoaderWithContext<String, String>) (keys, environment) -> CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(Function.identity(), Function.identity(), (left, right) -> right, LinkedHashMap::new))),
                        (factory, options) -> DataLoaderFactory.newMappedDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newMappedDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().mappedBatchLoader(factory)
                ),
                new IdentityVariant<>(
                        "BatchPublisher",
                        (BatchPublisher<String, String>) (keys, subscriber) -> Flux.fromIterable(keys).subscribe(subscriber),
                        (factory, options) -> DataLoaderFactory.newPublisherDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newPublisherDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().publisherBatchLoader(factory)
                ),
                new IdentityVariant<>(
                        "BatchPublisherWithContext",
                        (BatchPublisherWithContext<String, String>) (keys, subscriber, environment) -> Flux.fromIterable(keys).subscribe(subscriber),
                        (factory, options) -> DataLoaderFactory.newPublisherDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newPublisherDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().publisherBatchLoader(factory)
                ),
                new IdentityVariant<>(
                        "MappedBatchPublisher",
                        (MappedBatchPublisher<String, String>) (keys, subscriber) -> Flux.fromIterable(keys.stream().map(key -> Map.entry(key, key)).collect(toList())).subscribe(subscriber),
                        (factory, options) -> DataLoaderFactory.newMappedPublisherDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newMappedPublisherDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().mappedPublisherBatchLoader(factory)
                ),
                new IdentityVariant<>(
                        "MappedBatchPublisherWithContext",
                        (MappedBatchPublisherWithContext<String, String>) (keys, subscriber, environment) -> Flux.fromIterable(keys.stream().map(key -> Map.entry(key, key)).collect(toList())).subscribe(subscriber),
                        (factory, options) -> DataLoaderFactory.newMappedPublisherDataLoader(factory, options),
                        (name, factory, options) -> DataLoaderFactory.newMappedPublisherDataLoader(name, factory, options),
                        factory -> DataLoaderFactory.<String, String>builder().mappedPublisherBatchLoader(factory)
                )
        );

        for (IdentityVariant<?> variant : variants) {
            assertIdentityVariant(variant, explicitOptions);
        }
    }

    @Test
    void runtime_behaviour_still_matches_for_list_and_mapped_loaders() {
        DataLoader<String, String> batchLoader = DataLoaderFactory.newDataLoader(keys -> CompletableFuture.completedFuture(new ArrayList<>(keys)));
        DataLoader<String, String> batchLoaderWithContext = DataLoaderFactory.newDataLoader((keys, environment) -> CompletableFuture.completedFuture(new ArrayList<>(keys)));
        DataLoader<String, String> mappedLoader = DataLoaderFactory.newMappedDataLoader(keys -> CompletableFuture.completedFuture(keys.stream()
                .filter(key -> !key.equals("missing"))
                .collect(Collectors.toMap(Function.identity(), key -> "mapped-" + key, (left, right) -> right, LinkedHashMap::new))));
        DataLoader<String, String> mappedLoaderWithContext = DataLoaderFactory.newMappedDataLoader((keys, environment) -> CompletableFuture.completedFuture(keys.stream()
                .filter(key -> !key.equals("missing"))
                .collect(Collectors.toMap(Function.identity(), key -> "mapped-" + key, (left, right) -> right, LinkedHashMap::new))));

        assertThat(dispatchAndJoin(batchLoader.load("A"), batchLoader), equalTo("A"));
        assertThat(dispatchAndJoin(batchLoaderWithContext.load("B"), batchLoaderWithContext), equalTo("B"));
        assertThat(dispatchAndJoin(mappedLoader.load("A"), mappedLoader), equalTo("mapped-A"));
        assertThat(dispatchAndJoin(mappedLoader.load("missing"), mappedLoader), nullValue());
        assertThat(dispatchAndJoin(mappedLoaderWithContext.load("C"), mappedLoaderWithContext), equalTo("mapped-C"));
        assertThat(dispatchAndJoin(mappedLoaderWithContext.load("missing"), mappedLoaderWithContext), nullValue());
    }

    @Test
    void runtime_behaviour_still_matches_for_publisher_loaders() {
        DataLoader<String, String> publisherLoader = DataLoaderFactory.newPublisherDataLoader((keys, subscriber) -> Flux.fromIterable(keys.stream().map(key -> "publisher-" + key).collect(toList())).subscribe(subscriber));
        DataLoader<String, String> publisherLoaderWithContext = DataLoaderFactory.newPublisherDataLoader((keys, subscriber, environment) -> Flux.fromIterable(keys.stream().map(key -> "publisher-ctx-" + key).collect(toList())).subscribe(subscriber));
        DataLoader<String, String> mappedPublisherLoader = DataLoaderFactory.newMappedPublisherDataLoader((keys, subscriber) -> Flux.fromIterable(keys.stream()
                .filter(key -> !key.equals("missing"))
                .map(key -> Map.entry(key, "mapped-publisher-" + key))
                .collect(toList())).subscribe(subscriber));
        DataLoader<String, String> mappedPublisherLoaderWithContext = DataLoaderFactory.newMappedPublisherDataLoader((keys, subscriber, environment) -> Flux.fromIterable(keys.stream()
                .filter(key -> !key.equals("missing"))
                .map(key -> Map.entry(key, "mapped-publisher-ctx-" + key))
                .collect(toList())).subscribe(subscriber));

        assertThat(dispatchAndJoin(publisherLoader.load("A"), publisherLoader), equalTo("publisher-A"));
        assertThat(dispatchAndJoin(publisherLoaderWithContext.load("B"), publisherLoaderWithContext), equalTo("publisher-ctx-B"));
        assertThat(dispatchAndJoin(mappedPublisherLoader.load("C"), mappedPublisherLoader), equalTo("mapped-publisher-C"));
        assertThat(dispatchAndJoin(mappedPublisherLoader.load("missing"), mappedPublisherLoader), nullValue());
        assertThat(dispatchAndJoin(mappedPublisherLoaderWithContext.load("D"), mappedPublisherLoaderWithContext), equalTo("mapped-publisher-ctx-D"));
        assertThat(dispatchAndJoin(mappedPublisherLoaderWithContext.load("missing"), mappedPublisherLoaderWithContext), nullValue());
    }

    @Test
    void try_factory_runtime_behaviour_still_matches() {
        DataLoader<String, Integer> batchTryLoader = DataLoaderFactory.newDataLoaderWithTry(keys -> CompletableFuture.completedFuture(keys.stream().map(this::tryLength).collect(toList())));
        DataLoader<String, Integer> batchTryLoaderWithContext = DataLoaderFactory.newDataLoaderWithTry((keys, environment) -> CompletableFuture.completedFuture(keys.stream().map(this::tryLength).collect(toList())));
        DataLoader<String, Integer> mappedTryLoader = DataLoaderFactory.newMappedDataLoaderWithTry(keys -> CompletableFuture.completedFuture(keys.stream().collect(Collectors.toMap(Function.identity(), this::tryLength))));
        DataLoader<String, Integer> publisherTryLoader = DataLoaderFactory.newPublisherDataLoaderWithTry((keys, subscriber) -> Flux.fromIterable(keys.stream().map(this::tryLength).collect(toList())).subscribe(subscriber));

        assertThat(dispatchAndJoin(batchTryLoader.load("A"), batchTryLoader), equalTo(1));
        assertFutureFails(dispatch(batchTryLoader.load("fail"), batchTryLoader));

        assertThat(dispatchAndJoin(batchTryLoaderWithContext.load("BB"), batchTryLoaderWithContext), equalTo(2));
        assertFutureFails(dispatch(batchTryLoaderWithContext.load("fail"), batchTryLoaderWithContext));

        assertThat(dispatchAndJoin(mappedTryLoader.load("CCC"), mappedTryLoader), equalTo(3));
        assertFutureFails(dispatch(mappedTryLoader.load("fail"), mappedTryLoader));

        assertThat(dispatchAndJoin(publisherTryLoader.load("DDDD"), publisherTryLoader), equalTo(4));
        assertFutureFails(dispatch(publisherTryLoader.load("fail"), publisherTryLoader));
    }

    private Try<Integer> tryLength(String key) {
        if ("fail".equals(key)) {
            return Try.failed(new IllegalStateException(key));
        }
        return Try.succeeded(key.length());
    }

    private static <T> void assertIdentityVariant(IdentityVariant<T> variant, DataLoaderOptions explicitOptions) {
        DataLoader<String, String> noNameNullOptions = variant.noNameFactory.apply(variant.batchLoadFunction, null);
        assertThat(variant.label + " no-name null options name", noNameNullOptions.getName(), nullValue());
        assertThat(variant.label + " no-name null options options", noNameNullOptions.getOptions(), notNullValue());
        assertThat(variant.label + " no-name null options batch function", noNameNullOptions.getBatchLoadFunction(), sameInstance(variant.batchLoadFunction));

        DataLoader<String, String> noNameExplicitOptions = variant.noNameFactory.apply(variant.batchLoadFunction, explicitOptions);
        assertThat(variant.label + " no-name explicit options name", noNameExplicitOptions.getName(), nullValue());
        assertThat(variant.label + " no-name explicit options options", noNameExplicitOptions.getOptions(), sameInstance(explicitOptions));
        assertThat(variant.label + " no-name explicit options batch function", noNameExplicitOptions.getBatchLoadFunction(), sameInstance(variant.batchLoadFunction));

        DataLoader<String, String> namedLoader = variant.namedFactory.apply("named-" + variant.label, variant.batchLoadFunction, explicitOptions);
        assertThat(variant.label + " named loader name", namedLoader.getName(), equalTo("named-" + variant.label));
        assertThat(variant.label + " named loader options", namedLoader.getOptions(), sameInstance(explicitOptions));
        assertThat(variant.label + " named loader batch function", namedLoader.getBatchLoadFunction(), sameInstance(variant.batchLoadFunction));

        DataLoader<String, String> builtLoader = variant.builderFactory.apply(variant.batchLoadFunction)
                .name("built-" + variant.label)
                .options(explicitOptions)
                .build();
        assertThat(variant.label + " built loader name", builtLoader.getName(), equalTo("built-" + variant.label));
        assertThat(variant.label + " built loader options", builtLoader.getOptions(), sameInstance(explicitOptions));
        assertThat(variant.label + " built loader batch function", builtLoader.getBatchLoadFunction(), sameInstance(variant.batchLoadFunction));
    }

    private static <T> T dispatchAndJoin(CompletableFuture<T> future, DataLoader<?, ?> dataLoader) {
        return dispatch(future, dataLoader).join();
    }

    private static <T> CompletableFuture<T> dispatch(CompletableFuture<T> future, DataLoader<?, ?> dataLoader) {
        dataLoader.dispatch();
        await().until(future::isDone);
        return future;
    }

    private static void assertFutureFails(CompletableFuture<?> future) {
        assertThat(future.isCompletedExceptionally(), equalTo(true));
    }

    private static List<String> publicStaticMethodDescriptors(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()) && Modifier.isStatic(method.getModifiers()))
                .sorted(Comparator.comparing(Method::getName).thenComparing(DataLoaderFactoryTest::parameterTypeNames))
                .map(DataLoaderFactoryTest::descriptor)
                .collect(toList());
    }

    private static List<String> publicDeclaredMethodDescriptors(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .sorted(Comparator.comparing(Method::getName).thenComparing(DataLoaderFactoryTest::parameterTypeNames))
                .map(DataLoaderFactoryTest::descriptor)
                .collect(toList());
    }

    private static String parameterTypeNames(Method method) {
        return Arrays.stream(method.getParameterTypes()).map(Class::getTypeName).collect(Collectors.joining(","));
    }

    private static String descriptor(Method method) {
        return method.getName()
                + "|rawReturn=" + method.getReturnType().getTypeName()
                + "|genericReturn=" + method.getGenericReturnType().getTypeName()
                + "|rawParams=" + Arrays.stream(method.getParameterTypes()).map(Class::getTypeName).collect(Collectors.joining(",", "[", "]"))
                + "|genericParams=" + Arrays.stream(method.getGenericParameterTypes()).map(Type::getTypeName).collect(Collectors.joining(",", "[", "]"));
    }

    private static void assertHasPublicStaticMethod(Class<?> type, String methodName, Class<?>... parameterTypes) {
        Method method = getMethod(type, methodName, parameterTypes);
        assertThat(Modifier.isPublic(method.getModifiers()), equalTo(true));
        assertThat(Modifier.isStatic(method.getModifiers()), equalTo(true));
    }

    private static void assertHasPublicMethod(Class<?> type, String methodName, Class<?>... parameterTypes) {
        Method method = getMethod(type, methodName, parameterTypes);
        assertThat(Modifier.isPublic(method.getModifiers()), equalTo(true));
    }

    private static Method getMethod(Class<?> type, String methodName, Class<?>... parameterTypes) {
        try {
            return type.getDeclaredMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private interface NoNameFactory<T> {
        DataLoader<String, String> apply(T batchLoadFunction, DataLoaderOptions options);
    }

    private interface NamedFactory<T> {
        DataLoader<String, String> apply(String name, T batchLoadFunction, DataLoaderOptions options);
    }

    private static class IdentityVariant<T> {
        private final String label;
        private final T batchLoadFunction;
        private final NoNameFactory<T> noNameFactory;
        private final NamedFactory<T> namedFactory;
        private final Function<T, DataLoaderFactory.Builder<String, String>> builderFactory;

        private IdentityVariant(String label, T batchLoadFunction, NoNameFactory<T> noNameFactory, NamedFactory<T> namedFactory, Function<T, DataLoaderFactory.Builder<String, String>> builderFactory) {
            this.label = label;
            this.batchLoadFunction = batchLoadFunction;
            this.noNameFactory = noNameFactory;
            this.namedFactory = namedFactory;
            this.builderFactory = builderFactory;
        }
    }
}
