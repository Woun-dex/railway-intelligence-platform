package com.rail.platform.graph.infrastructure.topology;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Minimal, dependency-free CSV reader for GTFS {@code *.txt} files.
 *
 * <p>Handles the GTFS dialect of RFC&nbsp;4180: a header row of column names,
 * double-quoted fields, embedded commas inside quotes, and escaped quotes
 * ({@code ""}). Rows are streamed one at a time so the large {@code stop_times.txt}
 * of a full feed never has to be materialized. (GTFS does not use newlines inside
 * quoted fields, so line-oriented streaming is safe.)
 */
final class GtfsCsvReader {

    private GtfsCsvReader() {
    }

    /** A single data row, addressable by column name. */
    static final class Row {
        private final Map<String, Integer> header;
        private final List<String> fields;

        Row(Map<String, Integer> header, List<String> fields) {
            this.header = header;
            this.fields = fields;
        }

        String get(String column) {
            Integer i = header.get(column);
            if (i == null || i >= fields.size()) {
                return "";
            }
            return fields.get(i).trim();
        }

        int getInt(String column, int defaultValue) {
            String v = get(column);
            if (v.isEmpty()) {
                return defaultValue;
            }
            try {
                return Integer.parseInt(v);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
    }

    /** Parses the stream, invoking {@code consumer} once per data row. */
    static void forEach(InputStream in, Consumer<Row> consumer) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return;
            }
            headerLine = stripBom(headerLine);
            List<String> headerFields = splitCsv(headerLine);
            Map<String, Integer> header = new HashMap<>();
            for (int i = 0; i < headerFields.size(); i++) {
                header.put(headerFields.get(i).trim(), i);
            }
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                consumer.accept(new Row(header, splitCsv(line)));
            }
        }
    }

    private static String stripBom(String s) {
        return (!s.isEmpty() && s.charAt(0) == '﻿') ? s.substring(1) : s;
    }

    /** Quote-aware split of a single CSV line. */
    static List<String> splitCsv(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    cur.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    out.add(cur.toString());
                    cur.setLength(0);
                } else {
                    cur.append(c);
                }
            }
        }
        out.add(cur.toString());
        return out;
    }
}
