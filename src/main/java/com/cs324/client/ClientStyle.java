package com.cs324.client;

import javax.swing.*;
import javax.swing.border.AbstractBorder;
import java.awt.*;
import java.awt.geom.Path2D;

// Static style and painting helper holder for the client; it carries no job logic.
final class ClientStyle {
    // One palette and one font family, so no component can drift out of theme.
    static final Color BACKGROUND = new Color(244, 248, 255);
    static final Color INK = new Color(14, 28, 65);
    static final Color MUTED = new Color(91, 109, 151);
    static final Color LINE = new Color(213, 222, 242);
    static final Color BLUE = new Color(72, 91, 211);
    static final Color LIGHT_BLUE = new Color(235, 240, 255);
    static final Font BODY = new Font("Segoe UI", Font.PLAIN, 14);

    // Never instantiated: every widget and constant here is static.
    private ClientStyle() { }

    // Scrollable card host, so a short window scrolls the cards instead of crushing them.
    static final class Workspace extends JPanel implements Scrollable {
        Workspace() { super(new BorderLayout(0, 14)); setOpaque(false); }
        public Dimension getPreferredScrollableViewportSize() { return getPreferredSize(); }
        public int getScrollableUnitIncrement(Rectangle visible, int orientation, int direction) { return 20; }
        public int getScrollableBlockIncrement(Rectangle visible, int orientation, int direction) {
            return Math.max(20, visible.height - 40);
        }
        // Width always fills the viewport; height only when the content already fits.
        public boolean getScrollableTracksViewportWidth() { return true; }
        public boolean getScrollableTracksViewportHeight() {
            return getParent() != null && getParent().getHeight() >= getPreferredSize().height;
        }
    }

    // Antialiasing is enabled once here so every custom shape below stays smooth.
    static Graphics2D graphics(Graphics source) {
        Graphics2D g = (Graphics2D) source.create();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        return g;
    }

    // White section with a soft shadow, keeping layout decisions out of the painting.
    static JPanel card(LayoutManager layout) {
        JPanel panel = new JPanel(layout) {
            @Override
            protected void paintComponent(Graphics source) {
                Graphics2D g = graphics(source);
                // Stacked translucent fills fake a soft shadow without loading any image.
                for (int i = 5; i >= 1; i--) {
                    g.setColor(new Color(66, 94, 155, 3));
                    g.fillRoundRect(6 - i, 6 - i, getWidth() - 12 + i * 2,
                            getHeight() - 12 + i * 2, 28, 28);
                }
                g.setColor(Color.WHITE);
                g.fillRoundRect(3, 1, getWidth() - 6, getHeight() - 7, 26, 26);
                g.dispose();
            }
        };
        panel.setOpaque(false);
        panel.setBorder(BorderFactory.createEmptyBorder(20, 24, 24, 24));
        return panel;
    }

    // Decorative server graphic painted behind the heading, so no asset files are needed.
    static JPanel header() {
        JPanel panel = new JPanel(new BorderLayout(20, 0)) {
            @Override
            protected void paintComponent(Graphics source) {
                super.paintComponent(source);
                // Hide decoration on narrow windows so it cannot compete with the text.
                if (getWidth() < 950) return;
                Graphics2D g = graphics(source);
                int x = getWidth() - 225;
                int y = 55;
                g.setColor(new Color(230, 237, 254));
                g.fillOval(x - 30, y + 18, 210, 70);
                g.fillOval(x + 70, y, 75, 70);
                g.setColor(new Color(216, 225, 252));
                g.fillRoundRect(x + 45, y + 5, 85, 25, 9, 9);
                g.fillRoundRect(x + 45, y + 34, 85, 25, 9, 9);
                g.setColor(Color.WHITE);
                g.fillRoundRect(x + 55, y + 15, 19, 3, 3, 3);
                g.fillRoundRect(x + 55, y + 44, 19, 3, 3, 3);
                g.setColor(new Color(111, 198, 192));
                g.fillOval(x + 112, y + 14, 6, 6);
                g.fillOval(x + 112, y + 43, 6, 6);
                g.dispose();
            }
        };
        panel.setOpaque(false);
        return panel;
    }

    // Rounded outline for input controls; the dashed variant marks the empty state.
    static final class RoundBorder extends AbstractBorder {
        // Dashed marks the empty-state placeholder rather than a real input control.
        private final boolean dashed;
        RoundBorder(boolean dashed) { this.dashed = dashed; }
        @Override
        public Insets getBorderInsets(Component c) { return new Insets(7, 9, 7, 9); }
        @Override
        // The copy overload exists so callers can reuse an Insets they already hold.
        public Insets getBorderInsets(Component c, Insets insets) {
            insets.set(7, 9, 7, 9);
            return insets;
        }
        @Override
        public void paintBorder(Component c, Graphics source, int x, int y, int w, int h) {
            Graphics2D g = graphics(source);
            // Focus recolours the outline, so keyboard users can see where they are.
            g.setColor(c.hasFocus() ? BLUE : LINE);
            g.setStroke(dashed
                    ? new BasicStroke(1.2f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1, new float[]{4, 4}, 0)
                    : new BasicStroke(1.2f));
            g.drawRoundRect(x + 1, y + 1, w - 3, h - 3, 16, 16);
            g.dispose();
        }
    }

    // Guidance painted only while empty, so the hint can never be submitted by accident.
    static final class InputArea extends JTextArea {
        private String hint = "Enter integers separated by commas...";
        InputArea() { super(3, 35); }
        void setHint(String hint) { this.hint = hint; repaint(); }
        @Override
        protected void paintComponent(Graphics source) {
            super.paintComponent(source);
            // Any text at all retires the hint; there is no placeholder value to select.
            if (!getText().isEmpty()) return;
            Graphics2D g = graphics(source);
            g.setFont(getFont());
            g.setColor(new Color(145, 155, 181));
            Insets padding = getInsets();
            g.drawString(hint, padding.left, padding.top + g.getFontMetrics().getAscent());
            g.dispose();
        }
    }

    // Painted surface only; Swing still handles clicks, mnemonics and focus for these.
    static JButton button(String text, String symbol, boolean primary) {
        JButton button = new JButton(text, icon(symbol, 19, primary ? Color.WHITE : BLUE)) {
            @Override
            // Surface first, super afterwards, so the label and icon still render on top.
            protected void paintComponent(Graphics source) {
                Graphics2D g = graphics(source);
                boolean pressed = getModel().isPressed();
                boolean hover = getModel().isRollover();
                Color top = primary ? new Color(84, 103, 228) : new Color(247, 249, 255);
                Color bottom = primary ? BLUE : new Color(238, 243, 255);
                // Hover and press darken the gradient, so the button reads as interactive.
                if (pressed || hover) { top = primary ? BLUE.darker() : LIGHT_BLUE; }
                g.setPaint(new GradientPaint(0, 0, top, 0, getHeight(), bottom));
                g.fillRoundRect(1, 1, getWidth() - 3, getHeight() - 3, 19, 19);
                g.setColor(primary ? BLUE : LINE);
                g.drawRoundRect(1, 1, getWidth() - 3, getHeight() - 3, 19, 19);
                // A second ring marks focus, independent of the platform look and feel.
                if (hasFocus()) {
                    g.setColor(primary ? Color.WHITE : BLUE);
                    g.drawRoundRect(4, 4, getWidth() - 9, getHeight() - 9, 14, 14);
                }
                g.dispose();
                super.paintComponent(source);
            }
        };
        // Hand-rolled UI with content and paint disabled: the surface above is the button.
        button.setUI(new javax.swing.plaf.basic.BasicButtonUI());
        button.setContentAreaFilled(false);
        button.setBorderPainted(false);
        button.setFocusPainted(false);
        button.setFont(BODY.deriveFont(Font.BOLD));
        button.setForeground(primary ? Color.WHITE : new Color(54, 76, 141));
        button.setIconTextGap(10);
        button.setBorder(BorderFactory.createEmptyBorder(12, 18, 12, 18));
        button.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        return button;
    }

    // Small vector icons avoid external asset files and missing Unicode glyphs.
    static Icon icon(String name, int size, Color color) {
        return new Icon() {
            public int getIconWidth() { return size; }
            public int getIconHeight() { return size; }
            public void paintIcon(Component c, Graphics source, int x, int y) {
                // Copy, so the caller's transform and hints are not mutated while drawing.
                Graphics2D g = graphics(source);
                g.translate(x, y);
                // Paths are authored on a 24x24 grid, then scaled to the requested size.
                g.scale(size / 24.0, size / 24.0);
                g.setColor(color);
                g.setStroke(new BasicStroke(1.8f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
                // An unknown name falls through to the default document glyph.
                switch (name) {
                    case "play" -> g.fillPolygon(new int[]{6, 6, 20}, new int[]{3, 21, 12}, 3);
                    case "folder" -> {
                        Path2D p = new Path2D.Double();
                        p.moveTo(3, 7); p.lineTo(3, 4); p.lineTo(9, 4); p.lineTo(12, 7);
                        p.lineTo(21, 7); p.lineTo(21, 20); p.lineTo(3, 20); p.closePath(); g.draw(p);
                    }
                    case "trash" -> {
                        g.drawLine(4, 6, 20, 6); g.drawLine(9, 3, 15, 3);
                        g.drawRoundRect(6, 6, 12, 15, 2, 2);
                        g.drawLine(10, 10, 10, 17); g.drawLine(14, 10, 14, 17);
                    }
                    case "list" -> {
                        for (int row = 5; row <= 19; row += 7) {
                            g.fillOval(2, row - 1, 3, 3); g.drawLine(9, row, 21, row);
                        }
                    }
                    case "home" -> {
                        g.drawPolyline(new int[]{2, 12, 22}, new int[]{11, 3, 11}, 3);
                        g.drawPolyline(new int[]{5, 5, 10, 10, 15, 15, 19, 19},
                                new int[]{10, 21, 21, 14, 14, 21, 21, 10}, 8);
                    }
                    case "eye" -> {
                        Path2D p = new Path2D.Double(); p.moveTo(2, 12);
                        p.curveTo(7, 3, 17, 3, 22, 12); p.curveTo(17, 21, 7, 21, 2, 12);
                        g.draw(p); g.drawOval(9, 9, 6, 6);
                    }
                    case "info" -> {
                        g.drawOval(3, 3, 18, 18); g.fillOval(11, 6, 2, 2); g.drawLine(12, 11, 12, 17);
                    }
                    case "dot" -> g.fillOval(6, 6, 12, 12);
                    case "keyboard" -> {
                        g.drawRoundRect(1, 5, 22, 14, 3, 3);
                        for (int row = 8; row <= 11; row += 3)
                            for (int col = 5; col <= 19; col += 4) g.fillRect(col, row, 1, 1);
                        g.drawLine(7, 15, 17, 15);
                    }
                    default -> {
                        Path2D p = new Path2D.Double(); p.moveTo(5, 2); p.lineTo(15, 2);
                        p.lineTo(20, 7); p.lineTo(20, 22); p.lineTo(5, 22); p.closePath(); g.draw(p);
                        g.drawPolyline(new int[]{15, 15, 20}, new int[]{2, 7, 7}, 3);
                        g.drawLine(9, 11, 16, 11); g.drawLine(9, 15, 16, 15); g.drawLine(9, 19, 13, 19);
                    }
                }
                g.dispose();
            }
        };
    }

    // Section icon inside a tinted circle, matching the heading design.
    static JLabel badge(String symbol) {
        JLabel badge = new JLabel(icon(symbol, 24, BLUE), SwingConstants.CENTER) {
            @Override
            // Tinted disc painted before the label, so the glyph sits inside a badge.
            protected void paintComponent(Graphics source) {
                Graphics2D g = graphics(source);
                g.setColor(LIGHT_BLUE); g.fillOval(0, 0, 48, 48); g.dispose();
                super.paintComponent(source);
            }
        };
        badge.setPreferredSize(new Dimension(48, 48));
        return badge;
    }
}
