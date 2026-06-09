package org.dataloader;

import org.dataloader.annotations.PublicApi;
import org.jspecify.annotations.Nullable;

import static org.dataloader.impl.Assertions.nonNull;

/**
 * A factory class to create {@link DataLoader}s
 */
@SuppressWarnings("unused")
@PublicApi
public class DataLoaderFactory {

    private DataLoaderFactory() {
    }

    public static <K, V> DataLoader<K, V> newDataLoader(BatchLoader<K, V> batchLoadFunction) {
        return newDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newDataLoader(String name, BatchLoader<K, V> batchLoadFunction) {
        return newDataLoader(name, batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newDataLoader(BatchLoader<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newDataLoader(String name, BatchLoader<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newDataLoaderWithTry(BatchLoader<K, Try<V>> batchLoadFunction) {
        return newDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newDataLoaderWithTry(BatchLoader<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newDataLoaderWithTry(String name, BatchLoader<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newDataLoader(BatchLoaderWithContext<K, V> batchLoadFunction) {
        return newDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newDataLoader(BatchLoaderWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newDataLoader(String name, BatchLoaderWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newDataLoaderWithTry(BatchLoaderWithContext<K, Try<V>> batchLoadFunction) {
        return newDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newDataLoaderWithTry(BatchLoaderWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newDataLoaderWithTry(String name, BatchLoaderWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoader(MappedBatchLoader<K, V> batchLoadFunction) {
        return newMappedDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoader(MappedBatchLoader<K, V> batchLoadFunction, @Nullable DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoader(String name, MappedBatchLoader<K, V> batchLoadFunction, @Nullable DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoaderWithTry(MappedBatchLoader<K, Try<V>> batchLoadFunction) {
        return newMappedDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoaderWithTry(MappedBatchLoader<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoaderWithTry(String name, MappedBatchLoader<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoader(MappedBatchLoaderWithContext<K, V> batchLoadFunction) {
        return newMappedDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoader(MappedBatchLoaderWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoader(String name, MappedBatchLoaderWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoaderWithTry(MappedBatchLoaderWithContext<K, Try<V>> batchLoadFunction) {
        return newMappedDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoaderWithTry(MappedBatchLoaderWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedDataLoaderWithTry(String name, MappedBatchLoaderWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoader(BatchPublisher<K, V> batchLoadFunction) {
        return newPublisherDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoader(BatchPublisher<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoader(String name, BatchPublisher<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoaderWithTry(BatchPublisher<K, Try<V>> batchLoadFunction) {
        return newPublisherDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoaderWithTry(BatchPublisher<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoaderWithTry(String name, BatchPublisher<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoader(BatchPublisherWithContext<K, V> batchLoadFunction) {
        return newPublisherDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoader(BatchPublisherWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoader(String name, BatchPublisherWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoaderWithTry(BatchPublisherWithContext<K, Try<V>> batchLoadFunction) {
        return newPublisherDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoaderWithTry(BatchPublisherWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newPublisherDataLoaderWithTry(String name, BatchPublisherWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoader(MappedBatchPublisher<K, V> batchLoadFunction) {
        return newMappedPublisherDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoader(MappedBatchPublisher<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoader(String name, MappedBatchPublisher<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoaderWithTry(MappedBatchPublisher<K, Try<V>> batchLoadFunction) {
        return newMappedPublisherDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoaderWithTry(MappedBatchPublisher<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoaderWithTry(String name, MappedBatchPublisher<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoader(MappedBatchPublisherWithContext<K, V> batchLoadFunction) {
        return newMappedPublisherDataLoader(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoader(MappedBatchPublisherWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoader(String name, MappedBatchPublisherWithContext<K, V> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoaderWithTry(MappedBatchPublisherWithContext<K, Try<V>> batchLoadFunction) {
        return newMappedPublisherDataLoaderWithTry(batchLoadFunction, null);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoaderWithTry(MappedBatchPublisherWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(null, batchLoadFunction, options);
    }

    public static <K, V> DataLoader<K, V> newMappedPublisherDataLoaderWithTry(String name, MappedBatchPublisherWithContext<K, Try<V>> batchLoadFunction, DataLoaderOptions options) {
        return createDataLoader(nonNull(name), batchLoadFunction, options);
    }

    private static <K, V> DataLoader<K, V> createDataLoader(@Nullable String name, Object batchLoadFunction, @Nullable DataLoaderOptions options) {
        return new DataLoader<>(name, batchLoadFunction, options);
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static <K, V> Builder<K, V> builder(DataLoader<K, V> dataLoader) {
        return new Builder<>(dataLoader);
    }

    public static class Builder<K, V> {
        @Nullable String name;
        Object batchLoadFunction;
        DataLoaderOptions options = DataLoaderOptions.newDefaultOptions();

        Builder() {
        }

        Builder(DataLoader<?, ?> dataLoader) {
            this.name = dataLoader.getName();
            this.batchLoadFunction = dataLoader.getBatchLoadFunction();
            this.options = dataLoader.getOptions();
        }

        public Builder<K, V> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<K, V> options(DataLoaderOptions options) {
            this.options = options;
            return this;
        }

        public Builder<K, V> batchLoadFunction(Object batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> batchLoader(BatchLoader<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> batchLoader(BatchLoaderWithContext<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> mappedBatchLoader(MappedBatchLoader<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> mappedBatchLoader(MappedBatchLoaderWithContext<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> publisherBatchLoader(BatchPublisher<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> publisherBatchLoader(BatchPublisherWithContext<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> mappedPublisherBatchLoader(MappedBatchPublisher<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public Builder<K, V> mappedPublisherBatchLoader(MappedBatchPublisherWithContext<K, V> batchLoadFunction) {
            this.batchLoadFunction = batchLoadFunction;
            return this;
        }

        public DataLoader<K, V> build() {
            return createDataLoader(name, batchLoadFunction, options);
        }
    }
}