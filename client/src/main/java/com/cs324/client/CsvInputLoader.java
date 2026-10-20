package com.cs324.client;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

// Reads one or more CSV files into the single numeric payload a job needs.
// Deliberately free of Swing, so the parsing rules can be tested headlessly.
public final class CsvInputLoader {

    // Cap the combined size, not each file: the merged text is rendered in a text
    // area and shipped as one payload, so multi-select must not raise the limit.
    public static final long MAX_TOTAL_BYTES = 1_000_000L;

    private CsvInputLoader() {}

    // One validation path for typed text and for loaded files, so a payload cannot
    // pass one route and fail the other.
    public static long[] parse(String text, String operation) {
        long[] values = parseCells(text);
        applyShapeRules(values, operation);
        return values;
    }

    // Reads every file, concatenates in filename order and validates the merged
    // result, so the shape rules see the whole payload rather than each fragment.
    public static long[] loadMerged(List<File> files, String operation) throws IOException {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("Select at least one CSV file.");
        }

        // The chooser reports files in directory order, so sort to keep runs repeatable.
        List<File> ordered = new ArrayList<>(files);
        ordered.sort(Comparator.comparing(file -> file.getName()));

        long total = 0;
        List<Long> merged = new ArrayList<>();

        for (File file : ordered) {
            total += Files.size(file.toPath());

            // Stop before reading the rest, so an oversized selection fails fast.
            if (total > MAX_TOTAL_BYTES) {
                throw new IllegalArgumentException(
                        "The selected files add up to more than 1 MB; choose fewer or smaller files.");
            }

            String contents;
            try {
                contents = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            } catch (IOException exception) {
                // Name the file: with several selected, a bare IO message is useless.
                throw new IOException("Could not read " + file.getName() + ".", exception);
            }

            // Editors may save a UTF-8 byte-order mark, which is not a digit.
            if (contents.startsWith("\uFEFF")) {
                contents = contents.substring(1);
            }

            // Report which file broke, rather than only the first one selected.
            try {
                for (long value : parseCells(contents)) {
                    merged.add(value);
                }
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException(
                        file.getName() + ": " + exception.getMessage(), exception);
            }
        }

        long[] values = new long[merged.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = merged.get(i);
        }

        // Applied once, after merging: a two-value rule must judge the whole payload.
        applyShapeRules(values, operation);
        return values;
    }

    // Renders values back as CSV for the input area. Wrapping at 20 per line keeps a
    // large load readable and re-parses identically, since rows may span lines.
    public static String toCsv(long[] values) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                text.append(i % 20 == 0 ? "\n" : ",");
            }
            text.append(values[i]);
        }
        return text.toString();
    }

    // Cell-level parsing shared by typed text and every file: bare comma-separated
    // integers, so a quote means a real CSV dialect this loader does not accept.
    private static long[] parseCells(String text) {
        String cleaned = text == null ? "" : text.strip();

        if (cleaned.isEmpty()) {
            throw new IllegalArgumentException("Enter numbers or load a CSV file first.");
        }

        if (cleaned.contains("\"")) {
            throw new IllegalArgumentException("Use plain numeric CSV without quotes or headers.");
        }

        List<Long> values = new ArrayList<>();

        // Accept comma-separated values across multiple rows.
        for (String row : cleaned.split("\\R", -1)) {
            if (row.isBlank()) {
                throw new IllegalArgumentException("Remove empty rows from the input.");
            }

            // Keep trailing empty cells so inputs such as "3,8," are rejected.
            for (String cell : row.split(",", -1)) {
                String value = cell.strip();

                if (value.isEmpty()) {
                    throw new IllegalArgumentException(
                            "A value is missing between commas or at a row end.");
                }

                try {
                    values.add(Long.parseLong(value));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(
                            "Every value must be a whole number within "
                                    + "the supported 64-bit integer range.");
                }
            }
        }

        // Convert the temporary growable list into the numeric array callers use.
        long[] result = new long[values.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    // The only per-operation difference; the numbers themselves are parsed once.
    private static void applyShapeRules(long[] values, String operation) {
        if ("PRIMESUM".equals(operation)) {
            if (values.length != 2) {
                throw new IllegalArgumentException("PRIMESUM requires exactly two values: start,end.");
            }

            if (values[0] > values[1]) {
                throw new IllegalArgumentException("The start must be less than or equal to the end.");
            }
        }
    }
}
