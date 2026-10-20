package com.cs324.client;

import com.cs324.progress.proto.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;

// Owns progress streams; all Swing state is changed on the event thread.
final class JobProgressView implements AutoCloseable {
    private final DefaultTableModel jobs;
    private final Map<Integer, JobProgress> snapshots = new HashMap<>();
    private final Map<Integer, DefaultTableModel> details = new HashMap<>();
    private final Set<JobProgressSubscription> subscriptions = ConcurrentHashMap.newKeySet();
    private final Set<JDialog> dialogs = new HashSet<>();

    void clear() {
        close();
        snapshots.clear();
        details.clear();
        for (JDialog dialog : Set.copyOf(dialogs)) dialog.dispose();
        dialogs.clear();
    }

    JobProgressView(DefaultTableModel jobs) { this.jobs = jobs; }

    void install(JTable table) {
        if (table.getColumnCount() > 6)
            ClientStyle.installDetailsAction(table, 6, row -> showDetails(table, row));
        table.getColumnModel().getColumn(5).setPreferredWidth(160);
        table.getColumnModel().getColumn(5).setCellRenderer((t, value, selected, focus, row, col) -> {
            JPanel bar = new JPanel() {
                @Override protected void paintComponent(Graphics source) {
                    super.paintComponent(source);
                    Graphics2D g = ClientStyle.graphics(source);
                    g.setFont(ClientStyle.BODY.deriveFont(12f));
                    String text = value instanceof Number ? value + "%" : String.valueOf(value);
                    int labelWidth = g.getFontMetrics().stringWidth(text) + 10;
                    int width = Math.max(0, getWidth() - labelWidth - 12);
                    int modelRow = t.convertRowIndexToModel(row);
                    JobProgress snapshot = snapshots.get(modelRow);
                    int y = 5;
                    g.setColor(new Color(220, 233, 255));
                    g.fillRoundRect(4, y, width, 14, 14, 14);
                    g.setColor(Color.WHITE);
                    g.fillRoundRect(6, y + 2, Math.max(0, width - 4), 10, 10, 10);
                    int percent = value instanceof Number n ? Math.clamp(n.intValue(), 0, 100) : 0;
                    int completed = (int) (Math.max(0, width - 6) * percent / 100.0);
                    g.setPaint(percent == 100
                            ? new GradientPaint(0, y, new Color(45, 211, 156), 0, y + 14, new Color(16, 177, 119))
                            : new GradientPaint(0, y, new Color(80, 197, 255), 0, y + 14, ClientStyle.BLUE));
                    g.fillRoundRect(7, y + 2, completed, 10, 10, 10);
                    g.setColor(ClientStyle.INK);
                    g.drawString(text, width + 10, y + 12);
                    if (snapshot != null) {
                        long total = snapshot.getWorkersList().stream().mapToLong(WorkerProgress::getTotalUnits).sum();
                        long done = snapshot.getWorkersList().stream().mapToLong(WorkerProgress::getCompletedUnits).sum();
                        g.setFont(ClientStyle.BODY.deriveFont(9f));
                        g.setColor(ClientStyle.MUTED);
                        g.drawString(String.format("%,d / %,d units", done, total), 5, y + 27);
                    }
                    g.dispose();
                }
            };
            bar.setBackground(selected ? t.getSelectionBackground() : row % 2 == 0 ? Color.WHITE : new Color(242, 247, 255));
            bar.getAccessibleContext().setAccessibleName("Progress: " + value);
            bar.setToolTipText("Click to see completed work by worker");
            return bar;
        });
        table.addMouseListener(new MouseAdapter() {
            @Override public void mouseClicked(MouseEvent event) {
                int row = table.rowAtPoint(event.getPoint());
                int col = table.columnAtPoint(event.getPoint());
                if (row < 0 || col < 0 || table.convertColumnIndexToModel(col) != 5) return;
                showDetails(table, table.convertRowIndexToModel(row));
            }
        });
    }

    private void showDetails(JTable table, int row) {
        DefaultTableModel model = new DefaultTableModel(new String[]{"Assigned worker", "Completed values", "Total values", "Status"}, 0) {
            @Override public boolean isCellEditable(int r, int c) { return false; }
        };
        details.put(row, model);
        refreshDetails(row);
        JDialog dialog = new JDialog(SwingUtilities.getWindowAncestor(table), "Worker progress â€” job " + jobs.getValueAt(row, 0));
        dialog.add(new JScrollPane(new JTable(model)), BorderLayout.CENTER);
        dialog.add(new JLabel("Updates when a worker finishes its share. Unfinished shares move to their replacement worker."), BorderLayout.SOUTH);
        dialog.setSize(720, 260);
        dialog.setLocationRelativeTo(table);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialogs.add(dialog);
        dialog.addWindowListener(new WindowAdapter() {
            @Override public void windowClosed(WindowEvent event) { details.remove(row, model); dialogs.remove(dialog); }
        });
        dialog.setVisible(true);
    }

    private void refreshDetails(int row) {
        DefaultTableModel model = details.get(row);
        if (model == null) return;
        model.setRowCount(0);
        JobProgress snapshot = snapshots.get(row);
        if (snapshot == null) model.addRow(new Object[]{"Waiting for progress", "", "", ""});
        else for (WorkerProgress worker : snapshot.getWorkersList()) {
            model.addRow(new Object[]{worker.getWorkerId(), worker.getCompletedUnits(), worker.getTotalUnits(), worker.getStatus()});
        }
    }

    void watch(String host, int port, int jobId, int row, java.util.function.BooleanSupplier visible) {
        watch(host, port, jobId, row, visible, java.util.List::of);
    }

    void watch(String host, int port, int jobId, int row, java.util.function.BooleanSupplier visible,
            java.util.function.Supplier<java.util.List<com.cs324.registry.proto.WorkerEndpoint>> discovery) {
        if (!visible.getAsBoolean()) return;
        if (port <= 0) {
            SwingUtilities.invokeLater(() -> { if (visible.getAsBoolean()) jobs.setValueAt("Unavailable", row, 5); });
            return;
        }
        var holder = new java.util.concurrent.atomic.AtomicReference<JobProgressSubscription>();
        var subscription = new JobProgressSubscription(jobId,
                com.cs324.registry.proto.WorkerEndpoint.newBuilder().setHost(host).setGrpcPort(port).build(),
                discovery, visible, snapshot -> {
                SwingUtilities.invokeLater(() -> {
                    if (!visible.getAsBoolean() || holder.get().cancelled()) return;
                    snapshots.put(row, snapshot);
                    long total = snapshot.getWorkersList().stream().mapToLong(WorkerProgress::getTotalUnits).sum();
                    long done = snapshot.getWorkersList().stream().mapToLong(WorkerProgress::getCompletedUnits).sum();
                    int percent = total == 0 ? (snapshot.getFinished() ? 100 : 0) : (int) (100.0 * done / total);
                    jobs.setValueAt(percent, row, 5);
                    refreshDetails(row);
                });
            }, () -> SwingUtilities.invokeLater(() -> {
                if (visible.getAsBoolean() && !holder.get().cancelled()) jobs.setValueAt("Reconnecting", row, 5);
            }), subscriptions::remove);
        holder.set(subscription);
        subscriptions.add(subscription);
        subscription.start();
    }

    @Override public void close() { subscriptions.forEach(JobProgressSubscription::close); subscriptions.clear(); }
}
