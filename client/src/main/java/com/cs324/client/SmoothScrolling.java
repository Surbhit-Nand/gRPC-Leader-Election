package com.cs324.client;

import java.awt.Component;
import java.awt.Container;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;
import javax.swing.*;

// Short eased wheel movements, with precise trackpad deltas and nested scroll handoff.
final class SmoothScrolling {
    private static final String KEY = SmoothScrolling.class.getName();
    private final JScrollPane pane;
    private final Timer timer;
    private JScrollBar bar;
    private double position;
    private double target;

    static void install(Container root) {
        if (root instanceof JScrollPane pane && pane.getClientProperty(KEY) == null) {
            pane.putClientProperty(KEY, new SmoothScrolling(pane));
        }
        for (Component child : root.getComponents()) {
            if (child instanceof Container container) install(container);
        }
    }

    private SmoothScrolling(JScrollPane pane) {
        this.pane = pane;
        timer = new Timer(15, event -> step());
        timer.setCoalesce(true);
        pane.setWheelScrollingEnabled(false);
        pane.addMouseWheelListener(this::wheel);
        MouseAdapter stop = new MouseAdapter() {
            @Override public void mousePressed(MouseEvent event) { timer.stop(); }
        };
        pane.getVerticalScrollBar().addMouseListener(stop);
        pane.getHorizontalScrollBar().addMouseListener(stop);
        pane.addHierarchyListener(event -> { if (!pane.isShowing()) timer.stop(); });
    }

    private void wheel(MouseWheelEvent event) {
        if (event.isConsumed()) return;
        JScrollBar selected = event.isShiftDown() ? pane.getHorizontalScrollBar() : pane.getVerticalScrollBar();
        if (bar != selected || !timer.isRunning()) {
            timer.stop();
            bar = selected;
            position = target = bar.getValue();
        }
        double rotation = event.getPreciseWheelRotation();
        if (rotation != 0 && Math.signum(rotation) != Math.signum(target - position)) {
            position = target = bar.getValue();
        }
        double distance = event.getScrollType() == MouseWheelEvent.WHEEL_BLOCK_SCROLL
                ? Math.max(40, bar.getVisibleAmount() * 0.9)
                : Math.max(1, event.getScrollAmount()) * 18.0;
        double limit = Math.max(bar.getMinimum(), bar.getMaximum() - bar.getVisibleAmount());
        double next = Math.max(bar.getMinimum(), Math.min(limit, target + rotation * distance));
        if (next == target) {
            // A full table or text area must not trap wheel input intended for the page.
            Container parent = pane.getParent();
            while (parent != null) {
                if (parent instanceof JScrollPane outer && outer.getClientProperty(KEY) instanceof SmoothScrolling scrolling) {
                    scrolling.wheel(event);
                    return;
                }
                parent = parent.getParent();
            }
            return;
        }
        target = next;
        event.consume();
        if (!timer.isRunning()) {
            step(); // Immediate feedback before the first animation tick.
            if (Math.abs(target - position) >= 0.5) timer.start();
        }
    }

    private void step() {
        if (bar == null || bar.getValueIsAdjusting()) { timer.stop(); return; }
        target = Math.max(bar.getMinimum(), Math.min(target, bar.getMaximum() - bar.getVisibleAmount()));
        position += (target - position) * 0.4;
        if (Math.abs(target - position) < 0.5) { position = target; timer.stop(); }
        bar.setValue((int) Math.round(position));
    }
}
