package com.cs324.client;

import com.cs324.progress.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;

/** Owns progress streams; all Swing state is changed on the event thread. */
final class JobProgressView implements AutoCloseable {
    private final DefaultTableModel jobs;
    private final Map<Integer, JobProgress> snapshots = new HashMap<>();
    private final Map<Integer, DefaultTableModel> details = new HashMap<>();
    private final Set<ManagedChannel> channels = ConcurrentHashMap.newKeySet();
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
        table.getColumnModel().getColumn(5).setPreferredWidth(160);
        table.getColumnModel().getColumn(5).setCellRenderer((t, value, selected, focus, row, col) -> {
            JProgressBar bar = new JProgressBar(0, 100);
            bar.setStringPainted(true);
            bar.setValue(value instanceof Number n ? n.intValue() : 0);
            bar.setString(value instanceof Number ? value + "%" : String.valueOf(value));
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
        JDialog dialog = new JDialog(SwingUtilities.getWindowAncestor(table), "Worker progress — job " + jobs.getValueAt(row, 0));
        dialog.add(new JScrollPane(new JTable(model)), BorderLayout.CENTER);
        dialog.add(new JLabel("Updates when a worker finishes its share. Reassigned shares keep their original worker label."), BorderLayout.SOUTH);
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
        if (!visible.getAsBoolean()) return;
        if (port <= 0) {
            SwingUtilities.invokeLater(() -> { if (visible.getAsBoolean()) jobs.setValueAt("Unavailable", row, 5); });
            return;
        }
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        channels.add(channel);
        if (!visible.getAsBoolean()) { channels.remove(channel); channel.shutdownNow(); return; }
        JobProgressServiceGrpc.newStub(channel).watchJob(WatchJobRequest.newBuilder().setJobId(jobId).build(), new StreamObserver<>() {
            public void onNext(JobProgress snapshot) {
                SwingUtilities.invokeLater(() -> {
                    if (!visible.getAsBoolean()) return;
                    snapshots.put(row, snapshot);
                    long total = snapshot.getWorkersList().stream().mapToLong(WorkerProgress::getTotalUnits).sum();
                    long done = snapshot.getWorkersList().stream().mapToLong(WorkerProgress::getCompletedUnits).sum();
                    int percent = total == 0 ? (snapshot.getFinished() ? 100 : 0) : (int) (100.0 * done / total);
                    jobs.setValueAt(percent, row, 5);
                    refreshDetails(row);
                });
            }
            public void onError(Throwable error) {
                SwingUtilities.invokeLater(() -> { if (visible.getAsBoolean()) jobs.setValueAt("Stream interrupted", row, 5); });
                release();
            }
            public void onCompleted() { release(); }
            private void release() { channels.remove(channel); channel.shutdownNow(); }
        });
    }

    @Override public void close() { channels.forEach(ManagedChannel::shutdownNow); channels.clear(); }
}
