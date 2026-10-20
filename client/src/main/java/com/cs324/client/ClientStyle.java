package com.cs324.client;

import javax.swing.*;
import javax.swing.border.AbstractBorder;
import java.awt.*;
import java.awt.geom.Path2D;

// Static style and painting helper holder for the client; it carries no job logic.
final class ClientStyle {
    // One palette and one font family, so no component can drift out of theme.
    static final Color BACKGROUND = new Color(242, 242, 242);
    static final Color INK = new Color(24, 24, 24);
    static final Color MUTED = new Color(100, 100, 100);
    static final Color LINE = new Color(215, 215, 215);
    static final Color ACCENT = new Color(28, 28, 28);
    static final Color SURFACE = new Color(235, 235, 235);
    static final Font BODY = new Font("Segoe UI", Font.PLAIN, 14);

    // Never instantiated: every widget and constant here is static.
    private ClientStyle() { }

    // Keep standard Swing controls and dialogs in the same monochrome palette.
    static void installTheme() {
        javax.swing.plaf.metal.MetalLookAndFeel.setCurrentTheme(
                new javax.swing.plaf.metal.DefaultMetalTheme() {
                    private javax.swing.plaf.ColorUIResource shade(Color color) {
                        return new javax.swing.plaf.ColorUIResource(color);
                    }
                    @Override protected javax.swing.plaf.ColorUIResource getPrimary1() { return shade(MUTED); }
                    @Override protected javax.swing.plaf.ColorUIResource getPrimary2() { return shade(LINE); }
                    @Override protected javax.swing.plaf.ColorUIResource getPrimary3() { return shade(SURFACE); }
                    @Override protected javax.swing.plaf.ColorUIResource getSecondary1() { return shade(MUTED); }
                    @Override protected javax.swing.plaf.ColorUIResource getSecondary2() { return shade(LINE); }
                    @Override protected javax.swing.plaf.ColorUIResource getSecondary3() { return shade(BACKGROUND); }
                });
        try {
            UIManager.setLookAndFeel(new javax.swing.plaf.metal.MetalLookAndFeel());
        } catch (UnsupportedLookAndFeelException exception) {
            throw new IllegalStateException("Could not apply the client theme", exception);
        }
    }

    // Scrollable card host, so a short window scrolls the cards instead of crushing them.
    static final class Workspace extends JPanel implements Scrollable {
        Workspace() { super(new BorderLayout(0, 10)); setOpaque(false); }
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
                    g.setColor(new Color(0, 0, 0, 3));
                    g.fillRoundRect(6 - i, 6 - i, getWidth() - 12 + i * 2,
                            getHeight() - 12 + i * 2, 28, 28);
                }
                g.setColor(Color.WHITE);
                g.fillRoundRect(3, 1, getWidth() - 6, getHeight() - 7, 26, 26);
                g.dispose();
            }
        };
        panel.setOpaque(false);
        panel.setBorder(BorderFactory.createEmptyBorder(10, 14, 12, 14));
        return panel;
    }

    // A quiet chessboard accent in the monochrome palette.
    static JPanel header() {
        JPanel panel = new JPanel(new BorderLayout(20, 0)) {
            @Override
            protected void paintComponent(Graphics source) {
                super.paintComponent(source);
                // Hide decoration on narrow windows so it cannot compete with the text.
                if (getWidth() < 950) return;
                Graphics2D g = graphics(source);
                int x = getWidth() - 186;
                int y = Math.max(0, (getHeight() - 64) / 2);
                for (int row = 0; row < 2; row++) {
                    for (int col = 0; col < 5; col++) {
                        g.setColor((row + col) % 2 == 0 ? LINE : SURFACE);
                        g.fillRoundRect(x + col * 32, y + row * 32, 30, 30, 5, 5);
                    }
                }
                icon("pawn", 30, MUTED).paintIcon(this, g, x + 6, y + 25);
                icon("king", 48, ACCENT).paintIcon(this, g, x + 56, y + 3);
                icon("pawn", 30, MUTED).paintIcon(this, g, x + 124, y + 25);
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
            g.setColor(c.hasFocus() ? ACCENT : LINE);
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
            g.setColor(new Color(125, 125, 125));
            Insets padding = getInsets();
            g.drawString(hint, padding.left, padding.top + g.getFontMetrics().getAscent());
            g.dispose();
        }
    }

    // Painted surface only; Swing still handles clicks, mnemonics and focus for these.
    static JButton button(String text, String symbol, boolean primary) {
        JButton button = new JButton(text, icon(symbol, 19, primary ? Color.WHITE : ACCENT)) {
            @Override
            // Surface first, super afterwards, so the label and icon still render on top.
            protected void paintComponent(Graphics source) {
                Graphics2D g = graphics(source);
                boolean pressed = getModel().isPressed();
                boolean hover = getModel().isRollover();
                Color top = primary ? new Color(65, 65, 65) : new Color(253, 253, 253);
                Color bottom = primary ? ACCENT : new Color(240, 240, 240);
                // Hover and press darken the gradient, so the button reads as interactive.
                if (pressed || hover) { top = primary ? ACCENT.darker() : SURFACE; }
                g.setPaint(new GradientPaint(0, 0, top, 0, getHeight(), bottom));
                g.fillRoundRect(1, 1, getWidth() - 3, getHeight() - 3, 19, 19);
                g.setColor(primary ? ACCENT : LINE);
                g.drawRoundRect(1, 1, getWidth() - 3, getHeight() - 3, 19, 19);
                // A second ring marks focus, independent of the platform look and feel.
                if (hasFocus()) {
                    g.setColor(primary ? Color.WHITE : ACCENT);
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
        button.setFont(BODY.deriveFont(Font.BOLD, 12f));
        button.setForeground(primary ? Color.WHITE : new Color(40, 40, 40));
        button.setIconTextGap(10);
        button.setBorder(BorderFactory.createEmptyBorder(6, 12, 6, 12));
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
                    case "knight" -> {
                        Path2D p = new Path2D.Double();
                        p.moveTo(8, 2); p.lineTo(12, 5);
                        p.curveTo(18, 5, 20, 10, 19, 18);
                        p.lineTo(19, 19); p.lineTo(6, 19);
                        p.curveTo(6, 15, 11, 14, 12, 10);
                        p.lineTo(7, 12); p.lineTo(3, 10);
                        p.lineTo(7, 5); p.closePath();
                        // A small cutout forms the eye without depending on background colour.
                        java.awt.geom.Area piece = new java.awt.geom.Area(p);
                        piece.subtract(new java.awt.geom.Area(
                                new java.awt.geom.Ellipse2D.Double(8, 6, 1.6, 1.6)));
                        g.fill(piece);
                        g.fillRoundRect(5, 20, 15, 2, 1, 1);
                    }
                    case "king" -> {
                        g.drawLine(12, 2, 12, 7);
                        g.drawLine(9, 4, 15, 4);
                        Path2D p = new Path2D.Double();
                        p.moveTo(6, 8); p.quadTo(9, 6, 12, 9);
                        p.quadTo(15, 6, 18, 8); p.lineTo(16, 13);
                        p.lineTo(15, 15); p.lineTo(18, 19);
                        p.lineTo(6, 19); p.lineTo(9, 15); p.lineTo(8, 13);
                        p.closePath(); g.fill(p);
                        g.fillRoundRect(5, 20, 14, 2, 1, 1);
                    }
                    case "pawn" -> {
                        g.fillOval(8, 2, 8, 8);
                        g.fillRoundRect(7, 10, 10, 3, 2, 2);
                        Path2D p = new Path2D.Double();
                        p.moveTo(9, 12); p.quadTo(10, 16, 6, 19);
                        p.lineTo(18, 19); p.quadTo(14, 16, 15, 12);
                        p.closePath(); g.fill(p);
                        g.fillRoundRect(5, 20, 14, 2, 1, 1);
                    }
                    case "rook" -> {
                        g.fillRect(5, 3, 3, 5); g.fillRect(10, 3, 4, 5);
                        g.fillRect(16, 3, 3, 5);
                        g.fillRect(5, 7, 14, 3); g.fillRect(8, 9, 8, 9);
                        g.fillRoundRect(6, 18, 12, 2, 1, 1);
                        g.fillRoundRect(4, 21, 16, 2, 1, 1);
                    }
                    case "refresh" -> {
                        g.drawArc(4, 4, 16, 16, 35, 270);
                        g.drawPolyline(new int[]{15, 21, 21}, new int[]{4, 4, 10}, 3);
                    }
                    case "network" -> {
                        g.drawLine(12, 5, 5, 18); g.drawLine(12, 5, 20, 18); g.drawLine(5, 18, 20, 18);
                        g.fillOval(8, 1, 8, 8); g.fillOval(0, 14, 8, 8); g.fillOval(16, 14, 8, 8);
                    }
                    case "users" -> {
                        g.fillOval(5, 3, 7, 7); g.fillOval(15, 4, 6, 6);
                        g.fillRoundRect(2, 12, 13, 10, 7, 7); g.fillRoundRect(16, 12, 7, 9, 5, 5);
                    }
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
        JLabel badge = new JLabel(icon(symbol, 20, ACCENT), SwingConstants.CENTER) {
            @Override
            // Tinted disc painted before the label, so the glyph sits inside a badge.
            protected void paintComponent(Graphics source) {
                Graphics2D g = graphics(source);
                g.setColor(SURFACE); g.fillOval(0, 0, 36, 36); g.dispose();
                super.paintComponent(source);
            }
        };
        badge.setPreferredSize(new Dimension(36, 36));
        return badge;
    }

    static void styleTableHeader(JTable table) {
        table.getTableHeader().setPreferredSize(new Dimension(0, 30));
        table.getTableHeader().setDefaultRenderer(new javax.swing.table.DefaultTableCellRenderer() {
            @Override public Component getTableCellRendererComponent(JTable t, Object value,
                    boolean selected, boolean focus, int row, int column) {
                super.getTableCellRendererComponent(t, value, selected, focus, row, column);
                setBackground(SURFACE);
                setForeground(MUTED);
                setFont(BODY.deriveFont(Font.BOLD, 10f));
                setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 10));
                return this;
            }
        });
    }

    // A real cell editor makes the action usable by mouse and keyboard.
    static void installDetailsAction(JTable table, int column, java.util.function.IntConsumer action) {
        table.getColumnModel().getColumn(column).setCellRenderer((t, value, selected, focus, row, col) -> {
            JPanel cell = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 3));
            cell.setBackground(selected ? t.getSelectionBackground() : row % 2 == 0 ? Color.WHITE : new Color(248, 248, 248));
            JButton view = button("View", "eye", false);
            view.setFont(BODY.deriveFont(11f));
            view.setBorder(BorderFactory.createEmptyBorder(3, 7, 3, 7));
            cell.add(view);
            return cell;
        });
        class ActionEditor extends AbstractCellEditor implements javax.swing.table.TableCellEditor {
            final JButton button = button("View", "eye", false);
            int modelRow;
            ActionEditor() {
                button.addActionListener(event -> {
                    int selectedRow = modelRow;
                    fireEditingCanceled();
                    action.accept(selectedRow);
                });
            }
            public Object getCellEditorValue() { return null; }
            public Component getTableCellEditorComponent(JTable t, Object value,
                    boolean selected, int row, int col) {
                modelRow = t.convertRowIndexToModel(row);
                return button;
            }
        }
        table.getColumnModel().getColumn(column).setCellEditor(new ActionEditor());
    }

    static final class TagRenderer extends javax.swing.table.DefaultTableCellRenderer {
        private Color tint;
        @Override public Component getTableCellRendererComponent(JTable table, Object value,
                boolean selected, boolean focus, int row, int column) {
            super.getTableCellRendererComponent(table, value, selected, focus, row, column);
            String text = value == null ? "" : value.toString();
            tint = text.equals("Coordinator") ? INK : SURFACE;
            setFont(BODY.deriveFont(Font.BOLD, 10f));
            setIcon(text.equals("Coordinator") ? icon("king", 17, Color.WHITE)
                    : text.equals("Worker") || text.equals("Unreachable") ? icon("pawn", 17, INK) : null);
            setIconTextGap(5);
            setForeground(text.equals("Coordinator") ? Color.WHITE : INK);
            setBackground(selected ? table.getSelectionBackground() : row % 2 == 0 ? Color.WHITE : new Color(248, 248, 248));
            setBorder(BorderFactory.createEmptyBorder(0, 12, 0, 4));
            setToolTipText(text);
            return this;
        }
        @Override protected void paintComponent(Graphics source) {
            Graphics2D g = graphics(source);
            g.setColor(getBackground()); g.fillRect(0, 0, getWidth(), getHeight());
            g.setFont(getFont());
            g.setColor(tint);
            g.fillRoundRect(7, (getHeight() - 19) / 2,
                    Math.min(getWidth() - 10, g.getFontMetrics().stringWidth(getText()) + 12
                            + (getIcon() == null ? 0 : getIcon().getIconWidth() + getIconTextGap())), 19, 7, 7);
            g.dispose();
            setOpaque(false);
            super.paintComponent(source);
        }
    }

    // Status text is retained alongside colour for accessibility.
    static final class StatusRenderer extends JPanel implements javax.swing.table.TableCellRenderer {
        private String text = "";
        private Color foreground;
        private Color fill;
        StatusRenderer() { setFont(BODY.deriveFont(12f)); }
        @Override public Component getTableCellRendererComponent(JTable table, Object value,
                boolean selected, boolean focus, int row, int column) {
            text = value == null ? "" : value.toString();
            foreground = INK;
            fill = SURFACE;
            setBackground(selected ? table.getSelectionBackground() : row % 2 == 0 ? Color.WHITE : new Color(247, 247, 247));
            setToolTipText(text);
            getAccessibleContext().setAccessibleName(text);
            return this;
        }
        @Override protected void paintComponent(Graphics source) {
            super.paintComponent(source);
            Graphics2D g = graphics(source);
            g.setFont(getFont());
            int width = Math.min(getWidth() - 14, g.getFontMetrics().stringWidth(text) + 34);
            int y = (getHeight() - 22) / 2;
            g.setColor(fill); g.fillRoundRect(7, y, width, 22, 22, 22);
            g.setColor(foreground); g.fillOval(15, y + 5, 12, 12);
            g.setColor(Color.WHITE);
            if (text.equals("Completed") || text.equals("Alive")) {
                g.setStroke(new BasicStroke(1.4f));
                g.drawPolyline(new int[]{18, 20, 24}, new int[]{y + 11, y + 13, y + 9}, 3);
            } else {
                g.drawLine(21, y + 8, 21, y + 12); g.fillOval(20, y + 14, 2, 2);
            }
            g.setColor(foreground); g.drawString(text, 33, y + 15);
            g.dispose();
        }
    }
}
