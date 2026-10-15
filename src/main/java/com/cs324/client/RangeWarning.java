package com.cs324.client;

// Pre-submit cost warning for prime-range jobs. Pure logic, no Swing, so the
// tier boundaries are unit-tested headlessly; ClientApp only shows the dialog.
// Only PRIMESUM reaches workers as a range (ClientApp builds PrimeRange solely
// for it); MAX/PRIMECOUNT travel as lists and need no width warning.
// This warns, never refuses: every width up to the int limits works, slowly.
public final class RangeWarning {

    // Quiet below this width: seconds on a normal cluster, not worth a popup.
    static final long MINUTES_FROM_WIDTH = 1_000_000L;
    // Past this width the job likely runs for hours, not minutes.
    static final long HOURS_FROM_WIDTH = 100_000_000L;

    private RangeWarning() {}

    // Null when no dialog is needed; otherwise the confirm-dialog message.
    // Callers must have validated the shape already (exactly two ascending values).
    public static String warningFor(String operation, long[] values) {
        if (!"PRIMESUM".equals(operation) || values == null || values.length != 2) {
            return null;
        }
        long width = values[1] - values[0] + 1;
        if (width <= MINUTES_FROM_WIDTH) {
            return null;
        }
        String size = "%,d".formatted(width) + (width == 1 ? " number" : " numbers");
        if (width > HOURS_FROM_WIDTH) {
            return "This PRIMESUM scans " + size + " and will likely take hours — "
                    + "values near 2,147,483,647 cost the most each. Submit anyway?";
        }
        return "This PRIMESUM scans " + size + " and will likely take minutes "
                + "on the cluster. Submit anyway?";
    }
}
