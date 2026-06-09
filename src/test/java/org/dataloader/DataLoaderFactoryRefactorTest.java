package org.dataloader;

import org.dataloader.instrumentation.DataLoaderInstrumentation;
import org.dataloader.instrumentation.DataLoaderInstrumentationContext;
import org.dataloader.instrumentation.DataLoaderRegistryInstrumentationTest;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class DataLoaderFactoryRefactorTest {

    @Test
    public void testPublicApiSnapshot() {
        Method[] methods = DataLoaderFactory.class.getDeclaredMethods();
        List<Method> publicStaticMethods = Arrays.stream(methods)
                .filter(m -> Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers()))
                .sorted(Comparator.comparing(Method::getName).thenComparing(Method::getParameterCount))
                .collect(Collectors.toList());

        Set<String> methodSignatures = new HashSet<>();
        for (Method m : publicStaticMethods) {
            StringBuilder sig = new StringBuilder();
            sig.append(m.getName()).append("(");
            Type[] pTypes = m.getGenericParameterTypes();
            for (int i = 0; i < pTypes.length; i++) {
                sig.append(pTypes[i].getTypeName());
                if (i < pTypes.length - 1) sig.append(", ");
            }
            sig.append(") -> ").append(m.getGenericReturnType().getTypeName());
            methodSignatures.add(sig.toString());
        }

        // Assert core methods exist
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newDataLoader(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newDataLoaderWithTry(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newMappedDataLoader(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newMappedDataLoaderWithTry(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newPublisherDataLoader(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newPublisherDataLoaderWithTry(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newMappedPublisherDataLoader(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("newMappedPublisherDataLoaderWithTry(")));
        assertTrue(methodSignatures.stream().anyMatch(s -> s.startsWith("builder(")));

        // Builder API
        Method[] builderMethods = DataLoaderFactory.Builder.class.getDeclaredMethods();
        Set<String> builderMethodNames = Arrays.stream(builderMethods)
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.toSet());

        assertTrue(builderMethodNames.contains("name"));
        assertTrue(builderMethodNames.contains("options"));
        assertTrue(builderMethodNames.contains("batchLoadFunction"));
        assertTrue(builderMethodNames.contains("batchLoader"));
        assertTrue(builderMethodNames.contains("mappedBatchLoader"));
        assertTrue(builderMethodNames.contains("publisherBatchLoader"));
        assertTrue(builderMethodNames.contains("mappedPublisherBatchLoader"));
        assertTrue(builderMethodNames.contains("build"));
    }

    @Test
    public void testGenericInference() {
        // 1. BatchLoader
        DataLoader<String, Integer> dl1 = DataLoaderFactory.newDataLoader(keys -> CompletableFuture.completedFuture(Collections.emptyList()));
        // 2. BatchLoaderWithContext
        DataLoader<String, Integer> dl2 = DataLoaderFactory.newDataLoader((keys, env) -> CompletableFuture.completedFuture(Collections.emptyList()));
        // 3. MappedBatchLoader
        DataLoader<String, Integer> dl3 = DataLoaderFactory.newMappedDataLoader(keys -> CompletableFuture.completedFuture(Collections.emptyMap()));
        // 4. MappedBatchLoaderWithContext
        DataLoader<String, Integer> dl4 = DataLoaderFactory.newMappedDataLoader((keys, env) -> CompletableFuture.completedFuture(Collections.emptyMap()));
        
        // 5. Try variants
        DataLoader<String, Integer> dl5 = DataLoaderFactory.newDataLoaderWithTry(keys -> CompletableFuture.completedFuture(Collections.emptyList()));
        DataLoader<String, Integer> dl6 = DataLoaderFactory.newMappedDataLoaderWithTry(keys -> CompletableFuture.completedFuture(Collections.emptyMap()));
        
        assertNotNull(dl1);
        assertNotNull(dl2);
        assertNotNull(dl3);
        assertNotNull(dl4);
        assertNotNull(dl5);
        assertNotNull(dl6);
    }

    @Test
    public void testIdentityAndRuntime() {
        BatchLoader<String, String> bl = keys -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions opts = DataLoaderOptions.newOptions().setMaxBatchSize(10).build();
        
        DataLoader<String, String> dl1 = DataLoaderFactory.newDataLoader(bl);
        assertNull(dl1.getName());
        assertSame(bl, dl1.getBatchLoadFunction());
        
        DataLoader<String, String> dl2 = DataLoaderFactory.newDataLoader("my-loader", bl, opts);
        assertEquals("my-loader", dl2.getName());
        assertSame(bl, dl2.getBatchLoadFunction());
        assertSame(opts, dl2.getOptions());

        dl2.load("A");
        dl2.dispatchAndJoin();

        // null options
        DataLoader<String, String> dl3 = DataLoaderFactory.newDataLoader("null-opts", bl, null);
        assertNotNull(dl3.getOptions()); // DataLoader constructor replaces null with default
        
        // mapped
        MappedBatchLoader<String, String> mbl = keys -> {
            Map<String, String> map = new HashMap<>();
            keys.forEach(k -> {
                if (!k.equals("Missing")) {
                    map.put(k, k + "-val");
                }
            });
            return CompletableFuture.completedFuture(map);
        };
        DataLoader<String, String> dlMapped = DataLoaderFactory.newMappedDataLoader("mapped", mbl, opts);
        CompletableFuture<String> f = dlMapped.load("X");
        CompletableFuture<String> missing = dlMapped.load("Missing"); // missing key
        dlMapped.dispatchAndJoin();
        assertEquals("X-val", f.join());
        assertNull(missing.join());
    }

    @Test
    public void testBuilderRegression() {
        BatchLoader<String, String> bl = keys -> CompletableFuture.completedFuture(keys);
        DataLoaderOptions opts = DataLoaderOptions.newOptions().setMaxBatchSize(10).build();
        DataLoader<String, String> dl1 = DataLoaderFactory.newDataLoader("test", bl, opts);
        
        DataLoader<String, String> dl2 = DataLoaderFactory.builder(dl1).build();
        assertEquals("test", dl2.getName());
        assertSame(bl, dl2.getBatchLoadFunction());
        assertSame(opts, dl2.getOptions());
    }

    @Test
    public void testInstrumentationInteraction() {
        BatchLoader<String, String> bl = keys -> CompletableFuture.completedFuture(keys);
        DataLoader<String, String> dl = DataLoaderFactory.newDataLoader("instr-loader", bl);
        
        List<String> calls = new ArrayList<>();
        DataLoaderInstrumentation instrumentation = new DataLoaderInstrumentation() {
            @Override
            public DataLoaderInstrumentationContext<Object> beginLoad(DataLoader<?, ?> dataLoader, Object key, Object environment) {
                calls.add("beginLoad");
                return new DataLoaderInstrumentationContext<Object>() {
                    @Override
                    public void onDispatched() { calls.add("onLoadDispatched"); }
                    @Override
                    public void onCompleted(Object result, Throwable t) { calls.add("onLoadCompleted"); }
                };
            }

            @Override
            public DataLoaderInstrumentationContext<DispatchResult<?>> beginDispatch(DataLoader<?, ?> dataLoader) {
                calls.add("beginDispatch");
                return new DataLoaderInstrumentationContext<DispatchResult<?>>() {
                    @Override
                    public void onDispatched() { calls.add("onDispatchDispatched"); }
                    @Override
                    public void onCompleted(DispatchResult<?> result, Throwable t) { calls.add("onDispatchCompleted"); }
                };
            }
        };

        DataLoaderRegistry registry = DataLoaderRegistry.newRegistry()
                .instrumentation(instrumentation)
                .build();
        registry.register("instr-loader", dl);
        
        DataLoader<String, String> regDl = registry.getDataLoader("instr-loader");
        regDl.load("X");
        regDl.dispatchAndJoin();
        
        assertTrue(calls.indexOf("beginLoad") < calls.indexOf("beginDispatch"));
        assertTrue(calls.contains("onLoadCompleted"));
        assertTrue(calls.contains("onDispatchCompleted"));
    }
}
