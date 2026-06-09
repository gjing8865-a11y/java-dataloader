package org.dataloader;

import org.dataloader.annotations.Internal;
import org.jspecify.annotations.NullMarked;

import java.util.Objects;

@Internal
@NullMarked
final class CacheKeyWithContext<K> {

    private final K key;
    private final Object context;

    CacheKeyWithContext(K key, Object context) {
        this.key = key;
        this.context = context;
    }

    K getKey() {
        return key;
    }

    Object getContext() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKeyWithContext<?> that = (CacheKeyWithContext<?>) o;
        return Objects.equals(key, that.key) && Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, context);
    }

    @Override
    public String toString() {
        return "CacheKeyWithContext{key=" + key + ", context=" + context + "}";
    }
}
