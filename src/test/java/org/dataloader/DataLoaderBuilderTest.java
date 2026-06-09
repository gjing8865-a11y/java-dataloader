package org.dataloader;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DataLoaderBuilderTest {

    BatchLoader<String, Object> batchLoader1 = keys -> null;

    BatchLoader<String, Object> batchLoader2 = keys -> null;

    DataLoaderOptions defaultOptions = DataLoaderOptions.newOptions().build();
    DataLoaderOptions differentOptions = DataLoaderOptions.newOptions().setCachingEnabled(false).build();

    @Test
    void canBuildNewDataLoaders() {
        DataLoaderFactory.Builder<String, Object> builder = DataLoaderFactory.builder();
        builder.options(differentOptions);
        builder.batchLoadFunction(batchLoader1);
        DataLoader<String, Object> dataLoader = builder.build();

        assertThat(dataLoader.getOptions(), equalTo(differentOptions));
        assertThat(dataLoader.getBatchLoadFunction(), equalTo(batchLoader1));

        builder = DataLoaderFactory.builder(dataLoader);
        dataLoader = builder.build();

        assertThat(dataLoader.getOptions(), equalTo(differentOptions));
        assertThat(dataLoader.getBatchLoadFunction(), equalTo(batchLoader1));

        builder = DataLoaderFactory.builder(dataLoader);
        builder.options(defaultOptions);
        builder.batchLoadFunction(batchLoader2);
        dataLoader = builder.build();

        assertThat(dataLoader.getOptions(), equalTo(defaultOptions));
        assertThat(dataLoader.getBatchLoadFunction(), equalTo(batchLoader2));
    }

    @Test
    void theDataLoaderCanTransform() {
        DataLoader<String, Object> dataLoaderOrig = DataLoaderFactory.newDataLoader(batchLoader1, defaultOptions);
        assertThat(dataLoaderOrig.getOptions(), equalTo(defaultOptions));
        assertThat(dataLoaderOrig.getBatchLoadFunction(), equalTo(batchLoader1));

        DataLoader<String, Object> dataLoaderTransformed = dataLoaderOrig.transform(it -> {
            it.options(differentOptions);
            it.batchLoadFunction(batchLoader2);
        });

        assertThat(dataLoaderTransformed, not(equalTo(dataLoaderOrig)));
        assertThat(dataLoaderTransformed.getOptions(), equalTo(differentOptions));
        assertThat(dataLoaderTransformed.getBatchLoadFunction(), equalTo(batchLoader2));

        dataLoaderOrig = DataLoaderFactory.newDataLoader(batchLoader1, defaultOptions);

        dataLoaderTransformed = dataLoaderOrig.transform(it -> {
            it.batchLoadFunction(batchLoader2);
        });

        assertThat(dataLoaderTransformed, not(equalTo(dataLoaderOrig)));
        assertThat(dataLoaderTransformed.getOptions(), equalTo(defaultOptions));
        assertThat(dataLoaderTransformed.getBatchLoadFunction(), equalTo(batchLoader2));
    }

    @Test
    void builder_existing_copies_batch_loader_identity_and_runtime_behaviour() {
        BatchLoader<String, String> batchLoader = keys -> CompletableFuture.completedFuture(new ArrayList<>(keys));
        DataLoader<String, String> original = DataLoaderFactory.newDataLoader("batch", batchLoader, differentOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertCopiedLoader(copy, original, () -> dispatchAndJoin(copy.load("A"), copy));
        assertThat(dispatchAndJoin(copy.load("B"), copy), equalTo("B"));
    }

    @Test
    void builder_existing_copies_mapped_batch_loader_identity_and_runtime_behaviour() {
        MappedBatchLoader<String, String> mappedBatchLoader = keys -> CompletableFuture.completedFuture(keys.stream()
                .collect(Collectors.toMap(Function.identity(), key -> "mapped-" + key, (left, right) -> right, LinkedHashMap::new)));
        DataLoader<String, String> original = DataLoaderFactory.newMappedDataLoader("mapped", mappedBatchLoader, differentOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertCopiedLoader(copy, original, () -> dispatchAndJoin(copy.load("A"), copy));
        assertThat(dispatchAndJoin(copy.load("B"), copy), equalTo("mapped-B"));
    }

    @Test
    void builder_existing_copies_batch_publisher_identity_and_runtime_behaviour() {
        BatchPublisher<String, String> publisher = (keys, subscriber) -> Flux.fromIterable(keys.stream().map(key -> "pub-" + key).collect(Collectors.toList())).subscribe(subscriber);
        DataLoader<String, String> original = DataLoaderFactory.newPublisherDataLoader("publisher", publisher, differentOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertCopiedLoader(copy, original, () -> dispatchAndJoin(copy.load("A"), copy));
        assertThat(dispatchAndJoin(copy.load("B"), copy), equalTo("pub-B"));
    }

    @Test
    void builder_existing_copies_mapped_batch_publisher_identity_and_runtime_behaviour() {
        MappedBatchPublisher<String, String> mappedPublisher = (keys, subscriber) -> Flux.fromIterable(keys.stream()
                .map(key -> Map.entry(key, "mapped-pub-" + key))
                .collect(Collectors.toList())).subscribe(subscriber);
        DataLoader<String, String> original = DataLoaderFactory.newMappedPublisherDataLoader("mapped-publisher", mappedPublisher, differentOptions);

        DataLoader<String, String> copy = DataLoaderFactory.builder(original).build();

        assertCopiedLoader(copy, original, () -> dispatchAndJoin(copy.load("A"), copy));
        assertThat(dispatchAndJoin(copy.load("B"), copy), equalTo("mapped-pub-B"));
    }

    private static void assertCopiedLoader(DataLoader<String, String> copy, DataLoader<String, String> original, Supplier<String> runtimeAssertion) {
        assertThat(copy.getName(), equalTo(original.getName()));
        assertThat(copy.getOptions(), sameInstance(original.getOptions()));
        assertThat(copy.getBatchLoadFunction(), sameInstance(original.getBatchLoadFunction()));
        assertThat(runtimeAssertion.get(), not(equalTo(null)));
    }

    private static <T> T dispatchAndJoin(CompletableFuture<T> future, DataLoader<?, ?> dataLoader) {
        dataLoader.dispatch();
        await().until(future::isDone);
        return future.join();
    }
}
