package com.cs324.client;

// Warn before expensive PRIMESUM ranges; validation remains separate from this optional warning.
public final class RangeWarning {

    // Quiet below this width: seconds on a normal cluster, not worth a popup.
    static final long MINUTES_FROM_WIDTH = 1_000_000L;
    // Past this width the job likely runs for hours, not minutes.
    static final long HOURS_FROM_WIDTH = 100_000_000L;

    private RangeWarning() {}

    // Return a warning or null; callers must first validate the two ascending range bounds.
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
            return "This PRIMESUM scans " + size + " and will likely take hours â€” "
                    + "values near 2,147,483,647 cost the most each. Submit anyway?";
        }
        return "This PRIMESUM scans " + size + " and will likely take minutes "
                + "on the cluster. Submit anyway?";
    }
}
