package com.citi.dataflowengine.processors.taskprocessors.datareaders;

import com.citi.dataflowengine.models.tasksettings.DataTaskSettings;
import com.citi.dataflowengine.models.tasksettings.subsettings.FileSettings;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Optimized JsonFileReader:
 *  - Supports normal JSON + jsonArrayPath
 *  - Supports JSONL (single objects, arrays, or arrays inside jsonPath)
 *  - Extracts ONLY required fields using precompiled paths
 *  - Supports special column: [CompleteJSONRecordAsText]
 */
@Slf4j
public class JsonFileReader implements DataReader {

    private final FileSettings fs;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<String> requiredColumns;
    private final Map<String, String[]> compiledPaths = new HashMap<>();

    private final boolean jsonl;
    private final String jsonPath;

    private BufferedReader jsonlReader;
    private Iterator<JsonNode> bufferedIterator;
    private Iterator<JsonNode> currentJsonlArray;

    private JsonParser streamingParser;
    private boolean streamingArrayMode;

    public static final String COMPLETE_JSON_COL = "[CompleteJSONRecordAsText]";

    public JsonFileReader(DataTaskSettings settings) {

        this.fs = settings.getSource().getFileSettings();
        this.requiredColumns = settings.getMappings()
                .stream()
                .map(m -> m.getSourceColumnName())
                .toList();

        this.jsonl = fs.isJSONL();
        this.jsonPath = StringUtils.trimToNull(fs.getJsonArrayPath());
    }

    @Override
    public void open() throws Exception {

        precompilePaths();

        if (jsonl) {
            openJsonlMode();
            return;
        }

        if (jsonPath != null) {
            openBufferedJsonArrayAtPath();
            return;
        }

        openStreamingMode();
    }

    // ─────────────────────────────────────────────────────────────
    // PRECOMPILE JSON PATHS
    // ─────────────────────────────────────────────────────────────
    private void precompilePaths() {

        for (String col : requiredColumns) {
            if (COMPLETE_JSON_COL.equals(col)) continue; // no path needed
            compiledPaths.put(col, col.split("\\."));
        }

        log.info("Compiled {} JSON column paths", compiledPaths.size());
    }

    // ─────────────────────────────────────────────────────────────
    // OPEN MODES
    // ─────────────────────────────────────────────────────────────
    private void openJsonlMode() throws Exception {

        jsonlReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(fs.getFilePath()), StandardCharsets.UTF_8));

        log.info("JsonFileReader opened in JSONL mode. path='{}', jsonPath='{}'",
                fs.getFilePath(), jsonPath);
    }

    private void openBufferedJsonArrayAtPath() throws Exception {

        try (InputStream in = new FileInputStream(fs.getFilePath())) {
            JsonNode root = mapper.readTree(in);

            JsonNode nodeAtPath = navigate(root, jsonPath);

            List<JsonNode> buf = new ArrayList<>();
            if (nodeAtPath != null && nodeAtPath.isArray()) {
                nodeAtPath.forEach(buf::add);
            } else if (nodeAtPath != null) {
                buf.add(nodeAtPath);
            }

            bufferedIterator = buf.iterator();

            log.info("JsonFileReader opened with jsonArrayPath='{}'. records={}",
                    jsonPath, buf.size());
        }
    }

    private void openStreamingMode() throws Exception {

        streamingParser = new JsonFactory().createParser(new FileInputStream(fs.getFilePath()));

        JsonToken t = streamingParser.nextToken();
        streamingArrayMode = (t == JsonToken.START_ARRAY);

        log.info("JsonFileReader opened in streaming mode. arrayMode={}", streamingArrayMode);
    }

    // ─────────────────────────────────────────────────────────────
    // READ BATCH
    // ─────────────────────────────────────────────────────────────
    @Override
    public List<Map<String, Object>> readBatch(int batchSize) throws Exception {

        List<Map<String, Object>> out = new ArrayList<>(batchSize);

        if (jsonl) {
            readJsonlBatch(out, batchSize);
        } else if (jsonPath != null) {
            readBufferedPathBatch(out, batchSize);
        } else {
            readStreamingBatch(out, batchSize);
        }

        return out.isEmpty() ? null : out;
    }

    // ─────────────────────────────────────────────────────────────
    // STREAMING MODE
    // ─────────────────────────────────────────────────────────────
    private void readStreamingBatch(List<Map<String, Object>> out, int batchSize) throws IOException {

        if (streamingParser == null) return;

        if (streamingArrayMode) {

            while (out.size() < batchSize) {

                JsonToken t = streamingParser.nextToken();
                if (t == null || t == JsonToken.END_ARRAY) break;

                if (t != JsonToken.START_OBJECT) continue;

                JsonNode node = mapper.readTree(streamingParser);
                out.add(extractRow(node));
            }

        } else {

            while (out.size() < batchSize) {

                JsonNode node = mapper.readTree(streamingParser);
                if (node == null) break;

                out.add(extractRow(node));

                if (streamingParser.nextToken() == null) break;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    // BUFFERED MODE (jsonArrayPath)
    // ─────────────────────────────────────────────────────────────
    private void readBufferedPathBatch(List<Map<String, Object>> out, int batchSize) {

        if (bufferedIterator == null) return;

        while (bufferedIterator.hasNext() && out.size() < batchSize) {
            JsonNode node = bufferedIterator.next();
            out.add(extractRow(node));
        }
    }

    // ─────────────────────────────────────────────────────────────
    // JSONL MODE
    // ─────────────────────────────────────────────────────────────
    private void readJsonlBatch(List<Map<String, Object>> out, int batchSize) throws IOException {

        while (out.size() < batchSize) {

            if (currentJsonlArray != null && currentJsonlArray.hasNext()) {
                out.add(extractRow(currentJsonlArray.next()));
                continue;
            }

            currentJsonlArray = null;

            String line = jsonlReader.readLine();
            if (line == null) break;

            if (line.isBlank()) continue;

            JsonNode root = mapper.readTree(line);

            JsonNode node = jsonPath != null ? navigate(root, jsonPath) : root;
            if (node == null || node.isMissingNode()) continue;

            if (node.isArray()) {
                currentJsonlArray = node.elements();
                continue;
            }

            out.add(extractRow(node));
        }
    }

    // ─────────────────────────────────────────────────────────────
    // EXTRACT ONLY REQUIRED COLUMNS
    // ─────────────────────────────────────────────────────────────
    private Map<String, Object> extractRow(JsonNode node) {

        Map<String, Object> row = new LinkedHashMap<>();

        for (String col : requiredColumns) {

            if (COMPLETE_JSON_COL.equals(col)) {
                row.put(col, node.toString());
                continue;
            }

            String[] parts = compiledPaths.get(col);
            if (parts == null) {
                row.put(col, null);
                continue;
            }

            JsonNode cur = node;
            for (String p : parts) {
                if (cur == null) break;
                cur = cur.path(p);
            }

            if (cur == null || cur.isMissingNode() || cur.isNull()) {
                row.put(col, null);
            } else if (cur.isValueNode()) {
                row.put(col, cur.asText());
            } else {
                // nested object or array → store as raw JSON
                row.put(col, cur.toString());
            }
        }

        return row;
    }

    // ─────────────────────────────────────────────────────────────
    // UTIL: simple dot-path navigation
    // ─────────────────────────────────────────────────────────────
    private JsonNode navigate(JsonNode root, String path) {

        JsonNode cur = root;

        for (String part : path.split("\\.")) {
            if (cur == null) return null;
            cur = cur.path(part);
        }

        return cur;
    }

    @Override
    public void close() {
        try { if (jsonlReader != null) jsonlReader.close(); } catch (Exception ignored) {}
        try { if (streamingParser != null) streamingParser.close(); } catch (Exception ignored) {}
    }
}
