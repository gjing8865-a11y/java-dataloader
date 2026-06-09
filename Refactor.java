import java.nio.file.*;
import java.util.regex.*;

public class Refactor {
    public static void main(String[] args) throws Exception {
        String content = new String(Files.readAllBytes(Paths.get("src/main/java/org/dataloader/DataLoaderFactory.java")));
        
        Pattern pattern = Pattern.compile("(public static <K, V> DataLoader<K, V> \\w+\\([^)]+\\)\\s*\\{)([^}]+)\\}");
        Matcher matcher = pattern.matcher(content);
        StringBuffer sb = new StringBuilder();
        
        while (matcher.find()) {
            String sig = matcher.group(1);
            String body = matcher.group(2);
            
            Matcher m = Pattern.compile("public static <K, V> DataLoader<K, V> (\\w+)\\(([^)]+)\\)").matcher(sig);
            if (m.find()) {
                String methodName = m.group(1);
                String paramsStr = m.group(2);
                
                String[] params = paramsStr.split(",");
                String hasName = "false";
                String nameArg = "null";
                String optionsArg = "null";
                String batchArg = "null";
                
                for (String p : params) {
                    p = p.trim();
                    String[] parts = p.split(" ");
                    String paramName = parts[parts.length - 1];
                    if (paramName.equals("name")) {
                        hasName = "true";
                        nameArg = "name";
                    } else if (paramName.equals("options")) {
                        optionsArg = "options";
                    } else {
                        batchArg = paramName;
                    }
                }
                
                String newBody = "\n        return createHelper(" + nameArg + ", " + hasName + ", " + batchArg + ", " + optionsArg + ");\n    ";
                matcher.appendReplacement(sb, Matcher.quoteReplacement(sig + newBody + "}"));
            }
        }
        matcher.appendTail(sb);
        content = sb.toString();
        
        content = content.replaceAll("static <K, V> DataLoader<K, V> mkDataLoader\\(@Nullable String name, Object batchLoadFunction, @Nullable DataLoaderOptions options\\)\\s*\\{[^}]+\\}",
            "private static <K, V> DataLoader<K, V> createHelper(String name, boolean checkName, Object batchLoadFunction, DataLoaderOptions options) {\n" +
            "        if (checkName) {\n" +
            "            nonNull(name);\n" +
            "        }\n" +
            "        return new DataLoader<>(name, batchLoadFunction, options);\n" +
            "    }");
            
        content = content.replace("return mkDataLoader(name, batchLoadFunction, options);", "return createHelper(name, false, batchLoadFunction, options);");
        
        Files.write(Paths.get("src/main/java/org/dataloader/DataLoaderFactory.java"), content.getBytes());
    }
}
