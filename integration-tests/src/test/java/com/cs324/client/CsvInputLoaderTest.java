package com.cs324.client;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

// Merging contract: several files become one payload, and the rules judge the whole
// payload rather than each fragment, so a bad value is always reported against its file.
class CsvInputLoaderTest {

    // Writes a file the loader can read, keeping each test's setup to one line.
    private static File csv(Path dir, String name, String contents) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        return file.toFile();
    }

    // One file behaves exactly as before multi-select existed.
    @Test void singleFileLoadsUnchanged(@TempDir Path dir) throws IOException {
        File file = csv(dir, "single.csv", "3,7,12,45");

        assertArrayEquals(new long[]{3, 7, 12, 45},
                CsvInputLoader.loadMerged(List.of(file), "PRIMECOUNT"));
    }

    // Files merge in filename order, not the order they were selected in.
    @Test void mergesFilesInFilenameOrder(@TempDir Path dir) throws IOException {
        File second = csv(dir, "b.csv", "20,30");
        File first = csv(dir, "a.csv", "10");

        // Deliberately selected in the opposite order to the expected merge order.
        long[] merged = CsvInputLoader.loadMerged(List.of(second, first), "PRIMECOUNT");

        assertArrayEquals(new long[]{10, 20, 30}, merged);
    }

    // A single row of cells may span several lines, so files can be shaped freely.
    @Test void acceptsValuesAcrossMultipleRows(@TempDir Path dir) throws IOException {
        File file = csv(dir, "rows.csv", "1,2,3\n4,5\n6");

        assertArrayEquals(new long[]{1, 2, 3, 4, 5, 6},
                CsvInputLoader.loadMerged(List.of(file), "PRIMECOUNT"));
    }

    // An editor byte-order mark is not a digit and must not fail the first cell.
    @Test void stripsByteOrderMark(@TempDir Path dir) throws IOException {
        File file = csv(dir, "bom.csv", "\uFEFF7,8");

        assertArrayEquals(new long[]{7, 8},
                CsvInputLoader.loadMerged(List.of(file), "PRIMECOUNT"));
    }

    // The cap covers the selection, so several files cannot exceed what one file could.
    @Test void rejectsSelectionOverOneMegabyteTotal(@TempDir Path dir) throws IOException {
        String big = "1,".repeat((int) (CsvInputLoader.MAX_TOTAL_BYTES / 2)) + "1";
        File first = csv(dir, "a.csv", big);
        File second = csv(dir, "b.csv", big);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.loadMerged(List.of(first, second), "PRIMECOUNT"));
        assertTrue(failure.getMessage().contains("1 MB"), failure.getMessage());
    }

    // Exactly at the cap is still accepted; only exceeding it fails.
    @Test void acceptsSelectionAtTheCap(@TempDir Path dir) throws IOException {
        // "1\n" is two bytes, so this lands precisely on the limit.
        int rows = (int) (CsvInputLoader.MAX_TOTAL_BYTES / 2);
        File file = csv(dir, "cap.csv", "1\n".repeat(rows));

        long[] merged = CsvInputLoader.loadMerged(List.of(file), "PRIMECOUNT");

        assertEquals(rows, merged.length);
    }

    // With several files selected, the message has to identify the guilty one.
    @Test void namesTheFileThatFailedValidation(@TempDir Path dir) throws IOException {
        File good = csv(dir, "a.csv", "1,2");
        File bad = csv(dir, "b.csv", "3,oops");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.loadMerged(List.of(good, bad), "PRIMECOUNT"));
        assertTrue(failure.getMessage().startsWith("b.csv:"), failure.getMessage());
    }

    // The two-value rule judges the merged payload, not each file in turn.
    @Test void appliesPrimeSumRuleToTheMergedPayload(@TempDir Path dir) throws IOException {
        File first = csv(dir, "a.csv", "10");
        File second = csv(dir, "b.csv", "20");

        assertArrayEquals(new long[]{10, 20},
                CsvInputLoader.loadMerged(List.of(first, second), "PRIMESUM"));

        File third = csv(dir, "c.csv", "30");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.loadMerged(List.of(first, second, third), "PRIMESUM"));
        assertTrue(failure.getMessage().contains("exactly two values"), failure.getMessage());
    }

    // Merging can produce a descending pair that no single file contained.
    @Test void rejectsDescendingRangeFormedByMerging(@TempDir Path dir) throws IOException {
        File high = csv(dir, "a.csv", "500");
        File low = csv(dir, "b.csv", "100");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.loadMerged(List.of(low, high), "PRIMESUM"));
        assertTrue(failure.getMessage().contains("less than or equal"), failure.getMessage());
    }

    // The stricter dialect rules survive the move into the shared loader.
    @Test void stillRejectsQuotesHeadersAndEmptyCells() {
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.parse("\"3\",4", "PRIMECOUNT"));
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.parse("value\n3,4", "PRIMECOUNT"));
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.parse("3,,4", "PRIMECOUNT"));
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.parse("3,4,", "PRIMECOUNT"));
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.parse("3\n\n4", "PRIMECOUNT"));
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.parse("   ", "PRIMECOUNT"));
    }

    // MAX and PRIMECOUNT accept any non-empty merged list.
    @Test void mergedListSatisfiesTheOtherOperations(@TempDir Path dir) throws IOException {
        File file = csv(dir, "nums.csv", "3,7,12,45");

        assertArrayEquals(new long[]{3, 7, 12, 45},
                CsvInputLoader.loadMerged(List.of(file), "MAX"));
        assertArrayEquals(new long[]{3, 7, 12, 45},
                CsvInputLoader.loadMerged(List.of(file), "PRIMECOUNT"));
    }

    // An empty selection is a usage error, not a silently empty payload.
    @Test void rejectsEmptySelection() {
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.loadMerged(List.of(), "PRIMECOUNT"));
        assertThrows(IllegalArgumentException.class,
                () -> CsvInputLoader.loadMerged(null, "PRIMECOUNT"));
    }

    // Rendered CSV must survive a round trip, since the input area is re-parsed on submit.
    @Test void renderedCsvRoundTripsThroughTheParser() {
        long[] values = new long[45];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 7 - 3;
        }

        String text = CsvInputLoader.toCsv(values);

        assertTrue(text.contains("\n"), "long lists should wrap onto several rows");
        assertArrayEquals(values, CsvInputLoader.parse(text, "PRIMECOUNT"));
        assertEquals("-3", CsvInputLoader.toCsv(new long[]{-3}));
    }

    // Duplicate values across files are kept, matching single-file behaviour.
    @Test void keepsDuplicatesAcrossFiles(@TempDir Path dir) throws IOException {
        File first = csv(dir, "a.csv", "5,5");
        File second = csv(dir, "b.csv", "5");

        assertArrayEquals(new long[]{5, 5, 5},
                CsvInputLoader.loadMerged(List.of(first, second), "PRIMECOUNT"));
    }

    // A directory entry is not readable as text, so the failure names it.
    @Test void reportsUnreadableSelection(@TempDir Path dir) throws IOException {
        File missing = new File(dir.toFile(), "gone.csv");

        IOException failure = assertThrows(IOException.class,
                () -> CsvInputLoader.loadMerged(Arrays.asList(missing), "PRIMECOUNT"));
        assertTrue(failure.getMessage().contains("gone.csv"), failure.getMessage());
    }
}
