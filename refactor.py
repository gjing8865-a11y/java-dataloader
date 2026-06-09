import re

with open('src/main/java/org/dataloader/DataLoaderFactory.java', 'r') as f:
    content = f.read()

# We need to replace all `return newDataLoader(...)`, `return mkDataLoader(...)` inside the public static methods.

def replacer(match):
    # match group 1 is the method signature up to `{`
    # match group 2 is the body
    sig = match.group(1)
    body = match.group(2)
    
    # We figure out if there's a name parameter, and what the batchLoadFunction parameter name is
    # We can just look at the parameter list
    m = re.search(r'public static <K, V> DataLoader<K, V> (\w+)\(([^)]+)\)', sig)
    if not m:
        return match.group(0)
    
    method_name = m.group(1)
    params_str = m.group(2)
    
    params = [p.strip().split(' ')[-1] for p in params_str.split(',')]
    
    has_name = 'false'
    name_arg = 'null'
    has_options = 'false'
    options_arg = 'null'
    
    batch_arg = 'null'
    for p in params:
        if p == 'name':
            has_name = 'true'
            name_arg = 'name'
        elif p == 'options':
            has_options = 'true'
            options_arg = 'options'
        else:
            batch_arg = p
            
    new_body = f"\n        return createHelper({name_arg}, {has_name}, {batch_arg}, {options_arg});\n    "
    
    return sig + new_body + "}"

# replace the methods
pattern = re.compile(r'(public static <K, V> DataLoader<K, V> \w+\([^)]+\)\s*\{)([^}]+)\}')
new_content = pattern.sub(replacer, content)

# Now add createHelper and replace mkDataLoader
# Wait, mkDataLoader is package-private `static <K, V> DataLoader<K, V> mkDataLoader(...)`
# We'll just change `mkDataLoader` to `createHelper` with the new signature
mkDataLoader_pattern = re.compile(r'static <K, V> DataLoader<K, V> mkDataLoader\(@Nullable String name, Object batchLoadFunction, @Nullable DataLoaderOptions options\)\s*\{[^}]+\}')

create_helper_code = """private static <K, V> DataLoader<K, V> createHelper(String name, boolean checkName, Object batchLoadFunction, DataLoaderOptions options) {
        if (checkName) {
            nonNull(name);
        }
        return new DataLoader<>(name, batchLoadFunction, options);
    }"""

new_content = mkDataLoader_pattern.sub(create_helper_code, new_content)

# We also need to fix Builder.build() which calls mkDataLoader
new_content = new_content.replace("return mkDataLoader(name, batchLoadFunction, options);", "return createHelper(name, false, batchLoadFunction, options);")

with open('src/main/java/org/dataloader/DataLoaderFactory.java', 'w') as f:
    f.write(new_content)
