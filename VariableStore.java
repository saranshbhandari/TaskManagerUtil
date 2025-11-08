package com.example.vars;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * VariableStore
 *
 * Thread-safe store for variables scoped like ${Scope.Key} with support for:
 * - Dot + bracket access: ${Task1.RequestHeader[TestHeader]}, ${Task1.ResponseBody[0].key}
 * - Typed values: String/Number/Map/List/POJO/JSON/XML (Document)/converted ResultSet
 * - Interpolation in strings: resolveVariables("x=${Task1.K}")
 * - Configurable missing-variable policy: KEEP_AS_IS, REPLACE_WITH_EMPTY, THROW_ERROR
 */
public class VariableStore {

    // ===================== Public API =====================

    /** Behavior when a placeholder cannot be resolved. */
    public enum MissingVariablePolicy {
        KEEP_AS_IS,         // leave "${...}" unchanged
        REPLACE_WITH_EMPTY, // replace with ""
        THROW_ERROR         // throw VariableNotFoundException
    }

    /** Thrown when a variable cannot be found and policy is THROW_ERROR. */
    public static class VariableNotFoundException extends RuntimeException {
        public VariableNotFoundException(String message) { super(message); }
    }

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^{}]+)}");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // The store keeps only the BASE variable: ${Scope.Key} → value
    private final ConcurrentHashMap<String, Object> store = new ConcurrentHashMap<>();
    private volatile MissingVariablePolicy missingPolicy = MissingVariablePolicy.KEEP_AS_IS;

    public VariableStore() {}
    public VariableStore(MissingVariablePolicy policy) { this.missingPolicy = Objects.requireNonNull(policy); }

    public void setMissingVariablePolicy(MissingVariablePolicy policy) {
        this.missingPolicy = Objects.requireNonNull(policy);
    }

    /**
     * Add a variable using full name: "${Task1.ResponseBody}"
     * Overwrites any existing value for the same base variable.
     */
    public void addVariable(String variableName, Object value) {
        String baseKey = toBaseVariableKey(normalizeVarName(variableName));
        store.put(baseKey, value);
    }

    /**
     * Add a variable using scope and key: ("Task1", "ResponseBody")
     * Internally stored as "${Task1.ResponseBody}".
     */
    public void addVariable(String scope, String key, Object value) {
        String baseVar = "${" + Objects.requireNonNull(scope).trim() + "." + Objects.requireNonNull(key).trim() + "}";
        addVariable(baseVar, value);
    }

    /**
     * Insert a batch of variables. Keys may be "${Scope.Key}" or "Scope.Key".
     */
    public void addVariables(Map<String, Object> variables) {
        if (variables == null) return;
        for (Map.Entry<String, Object> e : variables.entrySet()) {
            addVariable(e.getKey(), e.getValue());
        }
    }

    /**
     * Convenience: convert a ResultSet to List<Map<String,Object>> and store it.
     * ResultSet is fully read and closed=false (caller manages) — this method does NOT close it.
     */
    public void addVariableFromResultSet(String variableName, ResultSet rs) throws Exception {
        addVariable(variableName, resultSetToList(rs));
    }

    /**
     * Get a value by expression, e.g.:
     * - "${Task1.ResponseHeader[TestHeader]}"
     * - "Task1.ResponseBody[0].key1"
     * Returns null if not found. (Interpolation policy applies only in resolveVariables)
     */
    public Object getValue(String expression) {
        try {
            return getValueInternal(expression, /*throwIfMissing*/ false);
        } catch (VariableNotFoundException ex) {
            // For direct getValue(), return null on missing
            return null;
        }
    }

    /**
     * Interpolate all ${...} placeholders in the input string using the configured policy.
     * - KEEP_AS_IS: leave unresolved as-is
     * - REPLACE_WITH_EMPTY: replace unresolved with ""
     * - THROW_ERROR: throw VariableNotFoundException
     */
    public String resolveVariables(String input) {
        if (input == null || input.isEmpty()) return input;
        Matcher m = PLACEHOLDER.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String expr = m.group(1);
            String fullExpr = "${" + expr + "}";
            Object val;
            try {
                val = getValueInternal(fullExpr, /*throwIfMissing*/ missingPolicy == MissingVariablePolicy.THROW_ERROR);
            } catch (VariableNotFoundException vex) {
                if (missingPolicy == MissingVariablePolicy.THROW_ERROR) {
                    throw vex;
                } else if (missingPolicy == MissingVariablePolicy.REPLACE_WITH_EMPTY) {
                    m.appendReplacement(sb, Matcher.quoteReplacement(""));
                    continue;
                } else { // KEEP_AS_IS
                    m.appendReplacement(sb, Matcher.quoteReplacement(fullExpr));
                    continue;
                }
            }
            String replacement = stringify(val);
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** Remove a specific variable (base variable only) — e.g., "${Task1.ResponseBody}" or "Task1.ResponseBody". */
    public void removeVariable(String variableName) {
        String base = toBaseVariableKey(normalizeVarName(variableName));
        store.remove(base);
    }

    /** Clear all variables. */
    public void clear() {
        store.clear();
    }

    // ===================== Internals =====================

    private Object getValueInternal(String expression, boolean throwIfMissing) {
        if (expression == null || expression.isEmpty()) {
            if (throwIfMissing) throw new VariableNotFoundException("Empty expression");
            return null;
        }

        // 1) Parse path into tokens
        Path path = Path.parse(expression);

        // 2) Fetch base var "${Scope.Key}"
        String baseVar = "${" + path.scope + "." + path.baseKey + "}";
        Object base = store.get(baseVar);
        if (base == null) {
            if (throwIfMissing) throw new VariableNotFoundException("Missing variable: " + baseVar);
            return null;
        }

        // 3) Drill down through remaining segments
        Object current = base;
        for (Segment seg : path.rest) {
            if (current == null) return null;

            if (current instanceof JsonNode) {
                current = navigateJson((JsonNode) current, seg);
            } else if (current instanceof Map) {
                current = navigateMap((Map<?, ?>) current, seg);
            } else if (current instanceof List) {
                current = navigateList((List<?>) current, seg);
            } else if (current.getClass().isArray()) {
                current = navigateArray(current, seg);
            } else if (current instanceof Document) {
                current = navigateXml((Document) current, seg, path.remainingAsXPathFrom(seg));
                // When XPath form is used, we resolve the *rest* in one go and stop.
                break;
            } else {
                current = navigatePojo(current, seg);
            }
        }
        return current;
    }

    // ---------- Navigation helpers ----------

    private Object navigateJson(JsonNode node, Segment seg) {
        if (seg.isIndex()) {
            JsonNode n = node.get(seg.asIndex());
            return n;
        } else {
            String key = seg.asKey();
            // permit ["some.key"] vs .key
            JsonNode n = node.get(key);
            if (n == null) n = node.path(key);
            return n.isMissingNode() ? null : n;
        }
    }

    private Object navigateMap(Map<?, ?> map, Segment seg) {
        if (seg.isIndex()) {
            // maps do not support positional index; treat as missing
            return null;
        } else {
            Object val = map.get(seg.asKey());
            return val;
        }
    }

    private Object navigateList(List<?> list, Segment seg) {
        if (!seg.isIndex()) return null;
        int idx = seg.asIndex();
        return (idx >= 0 && idx < list.size()) ? list.get(idx) : null;
    }

    private Object navigateArray(Object array, Segment seg) {
        if (!seg.isIndex()) return null;
        int idx = seg.asIndex();
        int len = Array.getLength(array);
        return (idx >= 0 && idx < len) ? Array.get(array, idx) : null;
    }

    /**
     * XML navigation:
     * - If a segment is a "key" starting with '/' or '.', we treat the *remaining* path as an XPath and evaluate it.
     *   Example usage: ${Task1.Xml[//root/items/item[1]/name]}
     * - Otherwise, simple child tag navigation by name (best-effort).
     */
    private Object navigateXml(Document doc, Segment firstSeg, String remainingAsXpath) {
        try {
            XPath xp = XPathFactory.newInstance().newXPath();
            String xpath;
            if (firstSeg.isKey() && looksLikeXPath(firstSeg.asKey())) {
                xpath = firstSeg.asKey(); // segment itself is an XPath
            } else if (remainingAsXpath != null && looksLikeXPath(remainingAsXpath)) {
                xpath = remainingAsXpath;
            } else {
                // Simple tag lookup (very basic): //tag
                xpath = "//" + firstSeg.asKey();
            }

            NodeList nodes = (NodeList) xp.evaluate(xpath, doc, XPathConstants.NODESET);
            if (nodes == null || nodes.getLength() == 0) return null;
            if (nodes.getLength() == 1) {
                return nodes.item(0).getTextContent();
            }

            // Return list of text contents if multiple matches
            List<String> list = new ArrayList<>(nodes.getLength());
            for (int i = 0; i < nodes.getLength(); i++) {
                list.add(nodes.item(i).getTextContent());
            }
            return list;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean looksLikeXPath(String s) {
        if (s == null) return false;
        return s.startsWith("/") || s.startsWith(".//") || s.startsWith("//");
    }

    private Object navigatePojo(Object pojo, Segment seg) {
        if (seg.isIndex()) {
            // Try get(index) if object exposes a method like get(int)
            try {
                Method m = pojo.getClass().getMethod("get", int.class);
                return m.invoke(pojo, seg.asIndex());
            } catch (Exception ignored) {}
            return null;
        } else {
            String key = seg.asKey();
            // Try getter: getKey() / isKey()
            String cap = key.substring(0,1).toUpperCase() + key.substring(1);
            String[] methods = new String[]{"get" + cap, "is" + cap, key}; // also allow direct no-arg method named 'key'
            for (String mn : methods) {
                try {
                    Method m = pojo.getClass().getMethod(mn);
                    if (m.getParameterCount() == 0) {
                        return m.invoke(pojo);
                    }
                } catch (Exception ignored) {}
            }
            // Try public field
            try {
                return pojo.getClass().getField(key).get(pojo);
            } catch (Exception ignored) {}
            return null;
        }
    }

    // ---------- Utilities ----------

    private static String normalizeVarName(String var) {
        String v = Objects.requireNonNull(var).trim();
        if (!v.startsWith("${")) {
            // allow "Task1.ResponseBody"
            v = "${" + v + "}";
        }
        return v;
    }

    /** Turn "${Task1.ResponseBody[0].key}" → "${Task1.ResponseBody}" */
    private static String toBaseVariableKey(String normalizedVar) {
        // Extract inside ${ ... }
        String inner = normalizedVar.substring(2, normalizedVar.length() - 1);
        // Split into tokens. First token = scope, second = baseKey; the rest is drill-down.
        Path p = Path.parse(normalizedVar);
        return "${" + p.scope + "." + p.baseKey + "}";
    }

    private static String stringify(Object val) {
        if (val == null) return "";
        if (val instanceof JsonNode) {
            try {
                return MAPPER.writeValueAsString(val);
            } catch (JsonProcessingException e) {
                return val.toString();
            }
        }
        if (val instanceof Document) {
            // very light representation
            return "[XML Document]";
        }
        if (val instanceof Collection || val.getClass().isArray() || val instanceof Map) {
            try {
                return MAPPER.writeValueAsString(val);
            } catch (Exception e) {
                return String.valueOf(val);
            }
        }
        return String.valueOf(val);
    }

    public static List<Map<String, Object>> resultSetToList(ResultSet rs) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++) {
                String name = md.getColumnLabel(i);
                if (name == null || name.isEmpty()) name = md.getColumnName(i);
                row.put(name, rs.getObject(i));
            }
            list.add(row);
        }
        return list;
    }

    // ===================== JSON/XML helpers =====================

    /** Convenience: parse JSON string into JsonNode before storing. */
    public static JsonNode parseJson(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON", e);
        }
    }

    /** Convenience: parse XML string into Document before storing. */
    public static Document parseXml(String xml) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            return dbf.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(xml.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid XML", e);
        }
    }

    // ===================== Path Parsing =====================

    private static final class Path {
        final String scope;   // e.g., Task1
        final String baseKey; // e.g., ResponseBody
        final List<Segment> rest; // further segments like [0], .key, [Name]

        private Path(String scope, String baseKey, List<Segment> rest) {
            this.scope = scope;
            this.baseKey = baseKey;
            this.rest = rest;
        }

        /**
         * Parse expressions like:
         *  - ${Task1.ResponseBody[0].key1}
         *  - Task1.ResponseHeader[TestHeader]
         *  - ${Task1.ResultSet[0][Name]}
         */
        static Path parse(String expr) {
            String s = expr.trim();
            if (s.startsWith("${") && s.endsWith("}")) {
                s = s.substring(2, s.length() - 1);
            }

            // Tokenize respecting brackets
            List<Segment> tokens = tokenize(s);

            if (tokens.size() < 2 || !tokens.get(0).isKey() || !tokens.get(1).isKey()) {
                throw new IllegalArgumentException("Expression must start with Scope.Key: " + expr);
            }
            String scope = tokens.get(0).asKey();
            String baseKey = tokens.get(1).asKey();
            List<Segment> rest = tokens.subList(2, tokens.size());
            return new Path(scope, baseKey, rest);
        }

        /** Remaining tokens rendered as a naive XPath if they already look like XPath in any segment. */
        String remainingAsXPathFrom(Segment current) {
            // Compose rest into a single string if any piece looks like XPath; otherwise return null.
            // This allows usage like ${Task1.Xml[//a/b/c]} where bracket contains full XPath.
            return null; // We compute XPath directly in navigateXml; no need to pre-build here.
        }

        private static List<Segment> tokenize(String s) {
            List<Segment> out = new ArrayList<>();
            StringBuilder buf = new StringBuilder();
            boolean inBracket = false;

            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (!inBracket) {
                    if (c == '.') {
                        if (buf.length() > 0) { out.add(Segment.key(buf.toString())); buf.setLength(0); }
                    } else if (c == '[') {
                        if (buf.length() > 0) { out.add(Segment.key(buf.toString())); buf.setLength(0); }
                        inBracket = true;
                    } else {
                        buf.append(c);
                    }
                } else { // inside [ ... ]
                    if (c == ']') {
                        String content = buf.toString();
                        out.add(parseBracketContent(content));
                        buf.setLength(0);
                        inBracket = false;
                    } else {
                        buf.append(c);
                    }
                }
            }
            if (buf.length() > 0) {
                out.add(Segment.key(buf.toString()));
            }
            return out;
        }

        private static Segment parseBracketContent(String raw) {
            String t = raw.trim();
            if (t.matches("\\d+")) {
                return Segment.index(Integer.parseInt(t));
            }
            return Segment.key(t);
        }
    }

    private static final class Segment {
        private final String key;
        private final Integer index;

        private Segment(String key, Integer index) { this.key = key; this.index = index; }
        static Segment key(String k) { return new Segment(k, null); }
        static Segment index(int i) { return new Segment(null, i); }

        boolean isIndex() { return index != null; }
        boolean isKey() { return key != null; }
        int asIndex() { return index; }
        String asKey() { return key; }
        public String toString() { return isIndex() ? ("[" + index + "]") : key; }
    }

    // ===================== Examples (commented) =====================
    /*
    public static void main(String[] args) throws Exception {
        VariableStore store = new VariableStore(MissingVariablePolicy.KEEP_AS_IS);

        // JSON example
        JsonNode json = parseJson("[{\"key1\":\"v1\"},{\"key1\":\"v2\"}]");
        store.addVariable("Task1", "ResponseBody", json);
        System.out.println(store.getValue("${Task1.ResponseBody[0].key1}")); // -> "v1" (JsonNode)

        String out = store.resolveVariables("Test value: ${Task1.ResponseBody[0].key1}");
        System.out.println(out); // -> Test value: "v1"

        // Map/List example
        Map<String,Object> hdr = new HashMap<>();
        hdr.put("TestHeader", "ABC123");
        store.addVariable("${Task1.ResponseHeader}", hdr);
        System.out.println(store.getValue("Task1.ResponseHeader[TestHeader]")); // ABC123

        // XML example (XPath inside brackets)
        String xml = "<root><items><item><name>A</name></item><item><name>B</name></item></items></root>";
        Document doc = parseXml(xml);
        store.addVariable("Task1", "ResponseXml", doc);
        System.out.println(store.getValue("${Task1.ResponseXml[//root/items/item/name]}")); // ["A","B"]

        // ResultSet example: store.addVariableFromResultSet("${Task1.ResultSet}", rs);
        // Then: ${Task1.ResultSet[0][Name]}
    }
    */
}
