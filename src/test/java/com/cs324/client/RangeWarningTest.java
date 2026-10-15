package com.cs324.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Tier boundaries of the pre-submit width warning: silent when fast,
// minutes-tier above 1M, hours-tier above 100M, never a refusal.
class RangeWarningTest {

    @Test
    void smallRangesStaySilent() {
        assertNull(RangeWarning.warningFor("PRIMESUM", new long[]{1, 1000}));
        assertNull(RangeWarning.warningFor("PRIMESUM", new long[]{1, 1_000_000}));
    }

    @Test
    void justOverOneMillionWarnsAboutMinutes() {
        String message = RangeWarning.warningFor("PRIMESUM", new long[]{1, 1_000_001});

        assertNotNull(message);
        assertTrue(message.contains("1,000,001 numbers"));
        assertTrue(message.contains("minutes"));
    }

    @Test
    void oneBillionWidthWarnsAboutHours() {
        String message = RangeWarning.warningFor("PRIMESUM", new long[]{1, 1_000_000_000});

        assertNotNull(message);
        assertTrue(message.contains("1,000,000,000 numbers"));
        assertTrue(message.contains("hours"));
    }

    @Test
    void topOfIntRangeWarnsAboutHours() {
        String message = RangeWarning.warningFor(
                "PRIMESUM", new long[]{1_147_483_647L, 2_147_483_647L});

        assertNotNull(message);
        assertTrue(message.contains("hours"));
    }

    @Test
    void nonRangeOperationsNeverWarn() {
        assertNull(RangeWarning.warningFor("MAX", new long[]{1, 1_000_000_000}));
        assertNull(RangeWarning.warningFor("PRIMECOUNT", new long[]{1, 1_000_000_000}));
        assertNull(RangeWarning.warningFor("PRIMESUM", null));
        assertNull(RangeWarning.warningFor("PRIMESUM", new long[]{1, 2, 3}));
    }
}
