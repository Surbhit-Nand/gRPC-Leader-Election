package com.cs324.client;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerRemote;
import com.cs324.worker.WorkerTaskCalculator;
import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static com.cs324.client.ClientStyle.*;

// Swing client: prepares input, submits to the current coordinator, then shows the result.
// Arguments: [bootstrapHost] [legacyUnusedPort] [callbackPort] [bootstrapGrpcPort].
public class ClientApp extends JFrame {

    // Colours, rounded panels and icons come from ClientStyle via the static import above.
    // Bootstrap endpoint, plus this client's gRPC callback port.
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final int clientPort;
    private final int bootstrapGrpcPort;

    // Keep controls as fields so button handlers can read input and update messages.
    private final JComboBox<String> jobTypeBox =
            new JComboBox<>(new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"});

    private final InputArea inputArea = new InputArea();
    private final JLabel exampleLabel = new JLabel();
    private final JLabel statusLabel =
            new JLabel("Ready to submit jobs");

    // Backend references (lazy: resolved on the first submission).
    private BootstrapRemote bootstrap;
    private io.grpc.Server resultServer;
    private com.cs324.computation.RegistryClient grpcDirectory;
    // Row index in the model of each in-flight job this window owns, keyed by job id.
    private final ConcurrentHashMap<Integer, Integer> rowByJobId = new ConcurrentHashMap<>();
    private final AtomicInteger nextRequestId = new AtomicInteger(1);

    // The model stores submitted jobs; JTable displays them. Users cannot edit the rows.
    private final DefaultTableModel jobModel = new DefaultTableModel(
            new String[]{"Job ID", "Operation", "Input", "Status", "Result", "Progress", "Actions"},
            0
    ) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return column == 6;
        }
    };

    private final JobProgressView progressView = new JobProgressView(jobModel);
    private volatile long historyGeneration;

    private void clearJobHistory() {
        historyGeneration++;
        rowByJobId.clear();
        progressView.clear();
        jobModel.setRowCount(0);
        jobCount.setText("0 jobs");
        historyLayout.show(historyContent, "empty");
        statusLabel.setText("Job history cleared. Submitted jobs continue on the cluster.");
    }
    private final JLabel jobCount = new JLabel("0 jobs");

    @Override public void dispose() {
        progressView.close();
        if (clusterTimer != null) clusterTimer.stop();
        if (resultServer != null) resultServer.shutdownNow();
        if (grpcDirectory != null) grpcDirectory.close();
        super.dispose();
    }
    // CardLayout displays either the empty-state message or the populated table.
    private final CardLayout historyLayout = new CardLayout();
    private final JPanel historyContent = new JPanel(historyLayout);

    // Cluster-activity view: worker snapshot table plus a feed of derived happenings.
    // Polling only — workers expose no extra state, events come from snapshot diffs.
    private final DefaultTableModel clusterModel = new DefaultTableModel(
            new String[]{"Worker", "Role", "Coordinator view", "Neighbours", "JAC", "Status", "Actions"},
            0
    ) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return column == 6;
        }
    };
    private final SystemLogPane clusterFeed = new SystemLogPane();
    private final JCheckBox autoScroll = new JCheckBox("Auto-scroll", true);
    private final JLabel clusterStatusLabel = new JLabel("Connecting to cluster...");
    private static final int CLUSTER_POLL_MS = 2000;
    private ClusterMonitor.ClusterSnapshot lastClusterSnapshot;
    private final Object clusterLock = new Object();
    // Newest worker-event sequence shown per worker; seeds silently on first sighting.
    private final Map<Integer, Long> eventSeqByWorker = new ConcurrentHashMap<>();
    private final AtomicBoolean clusterPollInFlight =
            new AtomicBoolean(false);
    private javax.swing.Timer clusterTimer;

    // One-pass construction: builds the three sections, then wires controls to their handlers.
    public ClientApp(String bootstrapHost, int bootstrapPort, int clientPort) {
        this(bootstrapHost, bootstrapPort, clientPort, com.cs324.common.Constants.DEFAULT_GRPC_PORT);
    }

    public ClientApp(String bootstrapHost, int bootstrapPort, int clientPort, int bootstrapGrpcPort) {
        super("CS324 Distributed Job Client");
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.clientPort = clientPort;
        this.bootstrapGrpcPort = bootstrapGrpcPort;

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(1000, 700));

        // Outer layout: heading at the top, workspace in the centre, status at the bottom.
        JPanel root = new JPanel(new BorderLayout(0, 10));
        root.setBackground(BACKGROUND);
        root.setBorder(BorderFactory.createEmptyBorder(8, 10, 8, 10));
        setContentPane(root);

        // Preserve the workspace heading and content with the refreshed palette.
        JPanel header = transparentPanel(new BorderLayout());
        header.setBorder(BorderFactory.createEmptyBorder(0, 10, 2, 10));
        JPanel heading = transparentPanel(null);
        heading.setLayout(new BoxLayout(heading, BoxLayout.Y_AXIS));
        JLabel breadcrumb = label("CS324   /   Distributed computing", 13, Font.PLAIN, MUTED);
        breadcrumb.setIcon(icon("network", 20, BLUE));
        breadcrumb.setIconTextGap(10);
        heading.add(breadcrumb);
        heading.add(Box.createVerticalStrut(4));
        heading.add(label("Job workspace", 24, Font.BOLD, INK));
        heading.add(Box.createVerticalStrut(3));
        heading.add(label("Prepare your data and review computational jobs.", 13, Font.PLAIN, MUTED));
        header.add(heading, BorderLayout.CENTER);
        root.add(header, BorderLayout.NORTH);

        // Rounded cards group the form and submitted job history.
        JPanel workspace = new Workspace();
        JPanel composer = card(new BorderLayout(0, 10));
        composer.add(sectionHeading("New job", "Choose an operation and provide input data.", "document"),
                BorderLayout.NORTH);

        // Operation selection determines the example text and validation rules.
        JPanel fields = transparentPanel(new BorderLayout(0, 8));
        JPanel selectorPanel = transparentPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        JLabel operationLabel = label("Operation   ", 14, Font.BOLD, INK);
        operationLabel.setLabelFor(jobTypeBox);
        selectorPanel.add(operationLabel);
        jobTypeBox.setFont(BODY);
        jobTypeBox.setBackground(Color.WHITE);
        jobTypeBox.setForeground(INK);
        jobTypeBox.setBorder(new RoundBorder(false));
        // A rounded dropdown keeps the same keyboard and selection behaviour as Swing.
        jobTypeBox.setUI(new javax.swing.plaf.basic.BasicComboBoxUI() {
            @Override
            protected JButton createArrowButton() {
                JButton arrow = new javax.swing.plaf.basic.BasicArrowButton(
                        SwingConstants.SOUTH, Color.WHITE, Color.WHITE, MUTED, Color.WHITE);
                arrow.setBorder(BorderFactory.createEmptyBorder(0, 8, 0, 8));
                arrow.setFocusable(false);
                return arrow;
            }
        });
        // Painted row states only; the combo box keeps stock selection and key handling.
        jobTypeBox.setRenderer(new DefaultListCellRenderer() {
            @Override
            public Component getListCellRendererComponent(JList<?> list, Object value,
                    int index, boolean selected, boolean focused) {
                super.getListCellRendererComponent(list, value, index, selected, focused);
                setBackground(selected ? LIGHT_BLUE : Color.WHITE);
                setForeground(INK);
                setBorder(BorderFactory.createEmptyBorder(5, 8, 5, 8));
                return this;
            }
        });
        jobTypeBox.setPreferredSize(new Dimension(200, 32));
        selectorPanel.add(jobTypeBox);
        JPanel operationRow = transparentPanel(new BorderLayout());
        operationRow.add(selectorPanel, BorderLayout.WEST);
        JLabel sourceHint = label("Manual entry or CSV files", 13, Font.PLAIN, MUTED);
        sourceHint.setIcon(icon("keyboard", 20, MUTED));
        sourceHint.setIconTextGap(9);
        operationRow.add(sourceHint, BorderLayout.EAST);
        fields.add(operationRow, BorderLayout.NORTH);

        // One scrollable input area is shared by typed data and imported CSV contents.
        inputArea.setLineWrap(true);
        inputArea.setWrapStyleWord(true);
        inputArea.setFont(BODY);
        inputArea.setForeground(INK);
        inputArea.setBackground(Color.WHITE);
        inputArea.setCaretColor(BLUE);
        inputArea.setSelectionColor(BLUE);
        inputArea.setSelectedTextColor(Color.WHITE);
        inputArea.setMargin(new Insets(5, 5, 5, 5));
        inputArea.getAccessibleContext().setAccessibleName("Job input data");

        JPanel inputPanel = transparentPanel(new BorderLayout(0, 8));
        exampleLabel.setFont(BODY.deriveFont(13f));
        exampleLabel.setForeground(MUTED);
        exampleLabel.setLabelFor(inputArea);
        inputPanel.add(exampleLabel, BorderLayout.NORTH);
        JScrollPane inputScroll = new JScrollPane(inputArea);
        inputScroll.setBorder(new RoundBorder(false));
        inputScroll.setBackground(Color.WHITE);
        inputScroll.getViewport().setBackground(Color.WHITE);
        inputScroll.setPreferredSize(new Dimension(700, 68));
        inputPanel.add(inputScroll, BorderLayout.CENTER);
        inputPanel.add(label("CSV: integers separated by commas; no headers or quotes. "
                        + "Select several files to merge them into one job. Up to 1 MB in total.",
                12, Font.PLAIN, MUTED), BorderLayout.SOUTH);
        fields.add(inputPanel, BorderLayout.CENTER);
        composer.add(fields, BorderLayout.CENTER);

        // File and clear actions stay secondary; submitting is the primary action.
        // Mnemonics provide keyboard shortcuts, such as Alt+L for loading a file on Windows.
        JButton loadCsvButton = button("Load CSVs", "folder", false);
        JButton submitButton = button("Submit job", "send", true);
        JButton clearButton = button("Clear input", "trash", false);
        loadCsvButton.setMnemonic('L');
        clearButton.setMnemonic('C');
        submitButton.setMnemonic('S');
        submitButton.setToolTipText("Validate input and submit the job to the current coordinator.");
        loadCsvButton.setToolTipText(
                "Choose one or more CSV files and merge them into a single job.");
        JPanel actions = transparentPanel(new BorderLayout(10, 0));
        JPanel secondaryActions = transparentPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        secondaryActions.add(loadCsvButton);
        secondaryActions.add(Box.createHorizontalStrut(10));
        secondaryActions.add(clearButton);
        actions.add(secondaryActions, BorderLayout.WEST);
        actions.add(submitButton, BorderLayout.EAST);
        composer.add(actions, BorderLayout.SOUTH);
        workspace.add(composer, BorderLayout.NORTH);

        // The second card switches between the empty state and submitted jobs.
        JPanel history = card(new BorderLayout(0, 8));
        JPanel historyHeading = transparentPanel(new BorderLayout());
        historyHeading.add(sectionHeading("Job history",
                "Track submitted jobs and their final results.", "list"), BorderLayout.CENTER);
        jobCount.setFont(BODY.deriveFont(12f));
        jobCount.setForeground(MUTED);
        JPanel historyActions = transparentPanel(new FlowLayout(FlowLayout.RIGHT, 12, 0));
        JButton clearHistoryButton = button("Clear job history", "trash", false);
        clearHistoryButton.setToolTipText("Clear displayed jobs; this does not cancel submitted work.");
        clearHistoryButton.addActionListener(event -> clearJobHistory());
        historyActions.add(jobCount);
        historyActions.add(clearHistoryButton);
        historyHeading.add(historyActions, BorderLayout.EAST);
        history.add(historyHeading, BorderLayout.NORTH);

        // Sorting changes the displayed order, while the model remains the source of row data.
        JTable jobTable = new JTable(jobModel);
        progressView.install(jobTable);
        jobTable.setFillsViewportHeight(true);
        jobTable.setAutoCreateRowSorter(true);
        jobTable.setFont(BODY.deriveFont(13f));
        jobTable.setRowHeight(40);
        jobTable.setShowVerticalLines(false);
        jobTable.setGridColor(LINE);
        jobTable.setIntercellSpacing(new Dimension(0, 1));
        jobTable.setSelectionBackground(LIGHT_BLUE);
        jobTable.setSelectionForeground(INK);
        jobTable.getTableHeader().setReorderingAllowed(false);
        jobTable.getTableHeader().setPreferredSize(new Dimension(0, 30));

        // Use white rows and subtle horizontal rules, preserving selection contrast.
        jobTable.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value,
                    boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(table, value, selected, focused, row, column);
                setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 10));
                if (!selected) {
                    setBackground(row % 2 == 0 ? Color.WHITE : new Color(248, 250, 255));
                    setForeground(INK);
                }
                setToolTipText(value == null ? null : value.toString());
                return this;
            }
        });
        // Plain headers retain an arrow showing the active sort direction.
        jobTable.getTableHeader().setDefaultRenderer(new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value,
                    boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(table, value, selected, focused, row, column);
                setBackground(new Color(245, 248, 255));
                setForeground(MUTED);
                setFont(BODY.deriveFont(Font.BOLD, 12f));
                setBorder(BorderFactory.createCompoundBorder(
                        BorderFactory.createMatteBorder(0, 0, 1, 0, LINE),
                        BorderFactory.createEmptyBorder(0, 10, 0, 10)));
                for (RowSorter.SortKey key : table.getRowSorter().getSortKeys()) {
                    if (key.getColumn() == table.convertColumnIndexToModel(column)) {
                        if (key.getSortOrder() == SortOrder.ASCENDING) setText(value + " \u2191");
                        if (key.getSortOrder() == SortOrder.DESCENDING) setText(value + " \u2193");
                        break;
                    }
                }
                return this;
            }
        });
        jobTable.getTableHeader().setFont(BODY.deriveFont(Font.BOLD, 12f));
        jobTable.getTableHeader().setBackground(Color.WHITE);
        jobTable.getTableHeader().setForeground(MUTED);
        // Explicit widths; automatic sizing would let the Input column collapse.
        jobTable.getColumnModel().getColumn(3).setCellRenderer(new StatusRenderer());
        jobTable.getColumnModel().getColumn(1).setCellRenderer(new TagRenderer());
        int[] widths = {70, 115, 170, 125, 140, 230, 90};
        for (int i = 0; i < widths.length; i++) {
            jobTable.getColumnModel().getColumn(i).setPreferredWidth(widths[i]);
        }

        // The header is the column view, so it stays pinned above the scrolling rows.
        JScrollPane resultsPane = new JScrollPane(jobTable);
        resultsPane.setColumnHeaderView(jobTable.getTableHeader());
        resultsPane.setBorder(BorderFactory.createEmptyBorder());
        resultsPane.getViewport().setBackground(Color.WHITE);
        resultsPane.setPreferredSize(new Dimension(750, 190));

        // Show instructions before the first job; submitJob() switches to the table.
        JPanel emptyState = transparentPanel(new GridBagLayout());
        emptyState.setBorder(new RoundBorder(true));
        JPanel emptyText = transparentPanel(null);
        emptyText.setLayout(new BoxLayout(emptyText, BoxLayout.Y_AXIS));
        JLabel emptyIcon = new JLabel(icon("document", 36, new Color(150, 164, 192)));
        JLabel emptyTitle = label("Your jobs and results will appear here", 16, Font.BOLD, INK);
        JLabel emptyHint = label("Choose an operation, add data, then submit your job.", 13, Font.PLAIN, MUTED);
        emptyTitle.setHorizontalAlignment(SwingConstants.CENTER);
        emptyHint.setHorizontalAlignment(SwingConstants.CENTER);
        emptyIcon.setAlignmentX(Component.CENTER_ALIGNMENT);
        emptyTitle.setAlignmentX(Component.CENTER_ALIGNMENT);
        emptyHint.setAlignmentX(Component.CENTER_ALIGNMENT);
        emptyText.add(emptyIcon);
        emptyText.add(Box.createVerticalStrut(10));
        emptyText.add(emptyTitle);
        emptyText.add(Box.createVerticalStrut(5));
        emptyText.add(emptyHint);
        emptyState.add(emptyText);
        historyContent.setBackground(Color.WHITE);
        historyContent.setPreferredSize(new Dimension(750, 125));
        historyContent.add(emptyState, "empty");
        historyContent.add(resultsPane, "table");
        historyLayout.show(historyContent, "empty");
        history.add(historyContent, BorderLayout.CENTER);
        workspace.add(history, BorderLayout.CENTER);
        workspace.add(buildClusterCard(), BorderLayout.SOUTH);
        // Scrolling wrapper keeps the cards legible on windows shorter than the content.
        JScrollPane workspaceScroll = new JScrollPane(workspace,
                JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        workspaceScroll.setBorder(BorderFactory.createEmptyBorder());
        workspaceScroll.getViewport().setBackground(BACKGROUND);
        workspaceScroll.getVerticalScrollBar().setUnitIncrement(20);
        root.add(workspaceScroll, BorderLayout.CENTER);
        SmoothScrolling.install(root);
        // Status line mirrors the newest job event; the hint explains where work runs.
        statusLabel.setText("Ready to prepare a job.");
        statusLabel.setFont(BODY.deriveFont(12f));
        statusLabel.setForeground(MUTED);
        statusLabel.setIcon(icon("dot", 18, new Color(31, 178, 141)));
        statusLabel.setIconTextGap(8);
        JPanel footer = transparentPanel(new BorderLayout(16, 0));
        footer.add(statusLabel, BorderLayout.CENTER);
        JLabel connectionHint = label("Jobs run on the worker cluster; results return here.",
                12, Font.PLAIN, MUTED);
        connectionHint.setIcon(icon("info", 17, MUTED));
        connectionHint.setIconTextGap(7);
        footer.add(connectionHint, BorderLayout.EAST);
        root.add(footer, BorderLayout.SOUTH);

        // Event handlers connect user actions to behaviour; building buttons alone does not.
        // Clearing input deliberately retains job history.
        jobTypeBox.addActionListener(event -> updateExample());
        loadCsvButton.addActionListener(event -> loadCsvFiles());
        submitButton.addActionListener(event -> submitJob());
        clearButton.addActionListener(event -> {
            inputArea.setText("");
            statusLabel.setText("Input cleared. Your job history is retained.");
            inputArea.requestFocusInWindow();
        });

        // Seed the guidance once, so the window never opens with an unexplained empty field.
        updateExample();
        // Cluster polling runs off the EDT and paints through renderClusterSnapshot.
        clusterTimer = new javax.swing.Timer(CLUSTER_POLL_MS, event -> refreshClusterNow());
        clusterTimer.setInitialDelay(500);
        clusterTimer.start();
        setSize(1180, 940);
        setLocationRelativeTo(null);
    }

    // Third workspace card: live worker table, happening feed, and refresh controls.
    private JPanel buildClusterCard() {
        JPanel cluster = card(new BorderLayout(0, 8));
        JPanel heading = transparentPanel(new BorderLayout());
        heading.add(sectionHeading("Cluster activity",
                "Live workers and what is happening between them.", "eye"), BorderLayout.CENTER);
        JButton refreshButton = button("Refresh", "refresh", false);
        refreshButton.setMnemonic('R');
        refreshButton.setToolTipText("Poll the cluster now instead of waiting for the next update.");
        refreshButton.addActionListener(event -> refreshClusterNow());
        heading.add(refreshButton, BorderLayout.EAST);
        cluster.add(heading, BorderLayout.NORTH);

        JTable clusterTable = new JTable(clusterModel);
        clusterTable.setFillsViewportHeight(true);
        clusterTable.setFont(BODY.deriveFont(11f));
        clusterTable.setRowHeight(28);
        clusterTable.setShowVerticalLines(false);
        clusterTable.setGridColor(LINE);
        clusterTable.setIntercellSpacing(new Dimension(0, 1));
        clusterTable.setSelectionBackground(LIGHT_BLUE);
        clusterTable.setSelectionForeground(INK);
        clusterTable.getTableHeader().setReorderingAllowed(false);
        clusterTable.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value,
                    boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(table, value, selected, focused, row, column);
                setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 10));
                if (!selected) {
                    setBackground(row % 2 == 0 ? Color.WHITE : new Color(248, 250, 255));
                    setForeground(INK);
                }
                setToolTipText(value == null ? null : value.toString());
                return this;
            }
        });
        styleTableHeader(clusterTable);
        clusterTable.getColumnModel().getColumn(5).setCellRenderer(new StatusRenderer());
        clusterTable.getColumnModel().getColumn(1).setCellRenderer(new TagRenderer());
        installDetailsAction(clusterTable, 6, row -> {
            StringBuilder details = new StringBuilder();
            for (int column = 0; column < 6; column++) {
                details.append(clusterModel.getColumnName(column)).append(": ")
                        .append(clusterModel.getValueAt(row, column)).append("\n");
            }
            JOptionPane.showMessageDialog(this, details.toString(), "Worker details", JOptionPane.INFORMATION_MESSAGE);
        });
        int[] widths = {70, 95, 105, 80, 40, 110, 80};
        for (int i = 0; i < widths.length; i++) {
            clusterTable.getColumnModel().getColumn(i).setPreferredWidth(widths[i]);
        }
        JScrollPane clusterPane = new JScrollPane(clusterTable);
        clusterPane.setBorder(BorderFactory.createEmptyBorder());
        clusterPane.getViewport().setBackground(Color.WHITE);
        clusterPane.setPreferredSize(new Dimension(610, 130));
        cluster.add(clusterPane, BorderLayout.CENTER);

        clusterFeed.setEditable(false);
        clusterFeed.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 11));
        clusterFeed.setMargin(new Insets(6, 10, 6, 10));
        ((javax.swing.text.DefaultCaret) clusterFeed.getCaret()).setUpdatePolicy(javax.swing.text.DefaultCaret.NEVER_UPDATE);
        clusterFeed.setForeground(INK);
        clusterFeed.setBackground(Color.WHITE);
        clusterFeed.setText("Waiting for the first cluster poll...");
        JScrollPane feedPane = new JScrollPane(clusterFeed);
        feedPane.setBorder(BorderFactory.createLineBorder(LINE));
        feedPane.getViewport().setBackground(Color.WHITE);
        feedPane.setPreferredSize(new Dimension(750, 120));

        JPanel clusterFooter = transparentPanel(new BorderLayout(16, 0));
        clusterStatusLabel.setFont(BODY.deriveFont(12f));
        clusterStatusLabel.setForeground(MUTED);
        clusterStatusLabel.setIcon(icon("dot", 18, new Color(31, 178, 141)));
        clusterStatusLabel.setIconTextGap(8);
        clusterFooter.add(clusterStatusLabel, BorderLayout.CENTER);

        JPanel log = card(new BorderLayout(0, 8));
        JPanel logHeader = new JPanel(new BorderLayout());
        logHeader.setBackground(LIGHT_BLUE);
        logHeader.setBorder(BorderFactory.createEmptyBorder(4, 10, 4, 8));
        JLabel logTitle = label("System log", 12, Font.BOLD, INK);
        logTitle.setIcon(icon("list", 15, MUTED));
        logHeader.add(logTitle, BorderLayout.WEST);
        autoScroll.setOpaque(false);
        autoScroll.setFont(BODY.deriveFont(11f));
        autoScroll.setForeground(MUTED);
        JPanel logControls = transparentPanel(new FlowLayout(FlowLayout.RIGHT, 12, 0));
        for (SystemLogPane.Level level : SystemLogPane.Level.values()) {
            JLabel key = new JLabel("● " + switch(level) {
                case SUCCESS -> "Success"; case INFO -> "Info";
                case WARNING -> "Recovery / election"; case ERROR -> "Failure";
            });
            key.setForeground(level.color);
            key.setFont(BODY.deriveFont(10f));
            logControls.add(key);
        }
        logControls.add(autoScroll);
        JButton clearLog = button("Clear log", "trash", false);
        clearLog.setToolTipText("Clear displayed log messages. New events will continue to appear.");
        clearLog.addActionListener(event -> clusterFeed.clearLog());
        logControls.add(clearLog);
        logHeader.add(logControls, BorderLayout.EAST);
        log.add(logHeader, BorderLayout.NORTH);
        log.add(feedPane, BorderLayout.CENTER);
        cluster.add(clusterFooter, BorderLayout.SOUTH);
        // Both panels remain independently scrollable; the divider can be resized.
        JSplitPane activity = new JSplitPane(JSplitPane.VERTICAL_SPLIT, cluster, log);
        activity.setBorder(BorderFactory.createEmptyBorder());
        activity.setOpaque(false);
        activity.setDividerSize(8);
        activity.setResizeWeight(0.5);
        activity.setDividerLocation(225);
        activity.setPreferredSize(new Dimension(900, 430));
        cluster.setMinimumSize(new Dimension(300, 170));
        log.setMinimumSize(new Dimension(300, 140));
        log.setPreferredSize(new Dimension(900, 195));
        JPanel activityRow = transparentPanel(new BorderLayout());
        activityRow.add(activity, BorderLayout.CENTER);
        return activityRow;
    }

    // One poll off the EDT: snapshot the cluster, diff against the last view,
    // then paint the table and feed on the event thread.
    private void refreshClusterNow() {
        if (!clusterPollInFlight.compareAndSet(false, true)) {
            return;
        }
        new Thread(() -> {
            try {
                connectBackend();
                ClusterMonitor.PollResult result = ClusterMonitor.pollWithEvents(bootstrap,
                        (workerId, address) -> {
                            try {
                                return lookupWorker(workerId, address);
                            } catch (Exception e) {
                                return null;
                            }
                        }, eventSeqByWorker);
                List<ClusterMonitor.ClusterEvent> heuristic;
                synchronized (clusterLock) {
                    heuristic = ClusterMonitor.diff(lastClusterSnapshot, result.snapshot(),
                            result.exactKindsByWorker());
                    lastClusterSnapshot = result.snapshot();
                }
                // Exact worker events first by timestamp; heuristic lines fill the gaps.
                List<ClusterMonitor.ClusterEvent> all =
                        new ArrayList<>(result.workerEvents());
                all.addAll(heuristic);
                all.sort((a, b) -> a.time().compareTo(b.time()));
                SwingUtilities.invokeLater(() -> renderClusterSnapshot(result.snapshot(), all));
            } catch (Exception e) {
                SwingUtilities.invokeLater(() ->
                        clusterStatusLabel.setText("Cluster unreachable — retrying..."));
            } finally {
                clusterPollInFlight.set(false);
            }
        }, "cluster-monitor-poll").start();
    }

    // Runs on the event thread: rebuilds the worker table and appends feed lines.
    private void renderClusterSnapshot(ClusterMonitor.ClusterSnapshot snapshot,
            List<ClusterMonitor.ClusterEvent> events) {
        clusterModel.setRowCount(0);
        for (ClusterMonitor.WorkerRow row : snapshot.rows()) {
            clusterModel.addRow(new Object[]{
                    "Worker " + row.workerId(),
                    ClusterMonitor.roleOf(row),
                    row.coordinatorView() == -1 ? "none" : "Worker " + row.coordinatorView(),
                    formatNeighbourIds(row.neighbours()),
                    row.alive() ? row.jac() : "-",
                    row.alive() ? "Alive" : "Unreachable"
            });
        }
        DateTimeFormatter clock =
                DateTimeFormatter.ofPattern("HH:mm:ss");
        clusterFeed.appendEvents(events, autoScroll.isSelected());
        int workers = snapshot.rows().size();
        String coordinator = snapshot.consensusCoordinator() == -1
                ? "no coordinator yet" : "coordinator Worker " + snapshot.consensusCoordinator();
        clusterStatusLabel.setText("Updated " + clock.format(LocalTime.now())
                + " · " + workers + (workers == 1 ? " worker" : " workers") + " · " + coordinator);
    }

    private static String formatNeighbourIds(Set<Integer> ids) {
        List<Integer> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);
        return sorted.toString();
    }

    // Groups components without covering the parent panel's background colour.
    private static JPanel transparentPanel(LayoutManager layout) {
        JPanel panel = new JPanel(layout);
        panel.setOpaque(false);
        return panel;
    }

    // Applies the shared font family, size and colour to one label.
    private static JLabel label(String text, int size, int style, Color color) {
        JLabel label = new JLabel(text);
        label.setFont(BODY.deriveFont(style, (float) size));
        label.setForeground(color);
        label.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 18));
        // BoxLayout honours the maximum size, so a label cannot stretch its row.
        label.setMaximumSize(new Dimension(Integer.MAX_VALUE, label.getPreferredSize().height));
        return label;
    }

    // Shared section heading: an icon, title and a short explanation.
    private static JPanel sectionHeading(String title, String description, String symbol) {
        JPanel heading = transparentPanel(new BorderLayout(14, 0));
        heading.add(badge(symbol), BorderLayout.WEST);
        JPanel text = transparentPanel(null);
        text.setLayout(new BoxLayout(text, BoxLayout.Y_AXIS));
        text.add(label(title, 18, Font.BOLD, INK));
        text.add(Box.createVerticalStrut(3));
        text.add(label(description, 13, Font.PLAIN, MUTED));
        heading.add(text, BorderLayout.CENTER);
        return heading;
    }

    // Updates input guidance when the operation changes; preserves the user's text.
    private void updateExample() {
        // Hint inside the field, example above it: both track the selected operation.
        String operation = (String) jobTypeBox.getSelectedItem();
        inputArea.setHint("PRIMESUM".equals(operation)
                ? "Enter start and end, separated by a comma..."
                : "Enter integers separated by commas...");

        if ("PRIMESUM".equals(operation)) {
            exampleLabel.setText(
                    "Enter start,end (inclusive). Example: 1,1000"
            );
        } else if ("PRIMECOUNT".equals(operation)) {
            exampleLabel.setText(
                    "Enter integers. Example: 2,3,4,5,5"
            );
        } else {
            exampleLabel.setText(
                    "Enter integers. Example: 3,7,12,45"
            );
        }
    }

    // Opens a multi-select chooser and merges every chosen file into one payload.
    // Cancellation or invalid data leaves the previous input untouched.
    private void loadCsvFiles() {
        JFileChooser chooser = new JFileChooser();
        chooser.setFileFilter(
                new FileNameExtensionFilter("CSV files (*.csv)", "csv")
        );
        chooser.setAcceptAllFileFilterUsed(false);
        // Several files become one job, so the user picks them all in one go.
        chooser.setMultiSelectionEnabled(true);

        // Approve-only chooser; a cancel returns without touching the current input.
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }

        File[] files = chooser.getSelectedFiles();
        String operation = (String) jobTypeBox.getSelectedItem();

        try {
            // CsvInputLoader validates the merged values, so nothing is replaced
            // until the whole selection is known to be submittable.
            long[] values = CsvInputLoader.loadMerged(Arrays.asList(files), operation);

            inputArea.setText(CsvInputLoader.toCsv(values));
            statusLabel.setText(files.length == 1
                    ? "Loaded " + files[0].getName() + " (" + values.length
                            + " values) — not submitted"
                    : "Loaded " + files.length + " files (" + values.length
                            + " values) — not submitted");
        } catch (IOException exception) {
            // The loader already names the file it could not read.
            showError(exception.getMessage());
        } catch (IllegalArgumentException exception) {
            showError(exception.getMessage());
        }
    }

    // Typed input and loaded files share CsvInputLoader, so both reach submitJob
    // through one set of rules and cannot disagree about what is valid.
    private long[] parseInput(String text) {
        return CsvInputLoader.parse(text, (String) jobTypeBox.getSelectedItem());
    }

    // Lazy backend setup under the monitor, so two submissions cannot create two registries.
    // The first submission creates our result registry and looks up the Bootstrap node.
    private synchronized void connectBackend() throws Exception {
        if (resultServer == null) {
            resultServer = io.grpc.ServerBuilder.forPort(clientPort)
                    .addService(new com.cs324.computation.ClientResultService((jobId, result) ->
                            SwingUtilities.invokeLater(() -> onResult(jobId, result))))
                    .build().start();
        }
        if (grpcDirectory == null) grpcDirectory = new com.cs324.computation.RegistryClient(bootstrapHost, bootstrapGrpcPort);
        if (bootstrap == null) {
            bootstrap = grpcDirectory;
        }
    }

    // Finds the current coordinator, asking the cluster to elect one if nobody knows yet.
    private WorkerRemote discoverCoordinator() throws Exception {
        // Bounded polling: a fresh cluster needs a moment to elect its first coordinator.
        for (int attempt = 0; attempt < 6; attempt++) {
            List<Integer> ids = bootstrap.getActiveWorkerIds();
            if (ids == null || ids.isEmpty()) {
                Thread.sleep(1000);
                continue;
            }
            for (Integer wid : ids) {
                WorkerAddress address = bootstrap.getWorkerAddress(wid);
                if (address == null) {
                    continue;
                }
                WorkerRemote candidate = lookupWorker(wid, address);
                if (candidate == null) {
                    continue;
                }
                int coordinatorId = candidate.getCurrentCoordinatorId();
                if (coordinatorId != -1) {
                    WorkerAddress coordAddress = bootstrap.getWorkerAddress(coordinatorId);
                    if (coordAddress != null) {
                        WorkerRemote coordinator = lookupWorker(coordinatorId, coordAddress);
                        if (coordinator != null) {
                            return coordinator;
                        }
                    }
                }
            }
            // No elected coordinator yet: trigger an election from the first live worker.
            if (!ids.isEmpty()) {
                Integer first = ids.get(0);
                WorkerAddress anyAddress = bootstrap.getWorkerAddress(first);
                WorkerRemote any = anyAddress != null ? lookupWorker(first, anyAddress) : null;
                if (any != null) {
                    try {
                        any.startElection();
                    } catch (Exception ignored) {
                        // The worker may have died between the lookup and this call.
                    }
                }
            }
            Thread.sleep(1200);
        }
        // Gave up; the caller reports the submission failed rather than waiting forever.
        return null;
    }

    private WorkerRemote lookupWorker(int workerId, WorkerAddress address) {
        // A worker can die between the directory listing and the lookup; that is not fatal.
        try {
            var worker = new com.cs324.computation.GrpcWorkerClient(grpcDirectory.worker(workerId));
            worker.ping();
            return worker;
        } catch (Exception e) {
            return null;
        }
    }

    // Builds the RMI payload: PrimeRange for PRIMESUM, int[] lists otherwise.
    private Object buildPayload(String operation, long[] values) {
        if ("PRIMESUM".equals(operation)) {
            return new WorkerTaskCalculator.PrimeRange(
                    Math.toIntExact(values[0]), Math.toIntExact(values[1]));
        }
        // MAX and PRIMECOUNT both reduce over a plain int list.
        int[] result = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = Math.toIntExact(values[i]);
        }
        return result;
    }

    // Validates and submits off the EDT, so a slow RMI call can never freeze the window.
    // The coordinator splits the job across the cluster and calls back with the aggregate.
    private void submitJob() {
        long[] values;
        String operation;
        Object payload;
        try {
            values = parseInput(inputArea.getText());
            operation = (String) jobTypeBox.getSelectedItem();
            // Math.toIntExact rejects values outside the 32-bit range.
            payload = buildPayload(operation, values);
        } catch (IllegalArgumentException | ArithmeticException exception) {
            showError(exception.getMessage());
            return;
        }

        // Wide prime ranges run long: warn first, still allow on confirm.
        String rangeWarning = RangeWarning.warningFor(operation, values);
        if (rangeWarning != null) {
            int answer = JOptionPane.showConfirmDialog(
                    this, rangeWarning, "Large range",
                    JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
            if (answer != JOptionPane.YES_OPTION) {
                statusLabel.setText("Submission cancelled — input kept.");
                return;
            }
        }

        String inputSummary = "PRIMESUM".equals(operation)
                ? values[0] + " to " + values[1]
                : values.length + " numbers";
        // Use a local reference while bootstrap allocates a unique cluster job ID.
        int requestId = -nextRequestId.getAndIncrement();
        AtomicInteger assignedId = new AtomicInteger(requestId);
        final String op = operation;
        final Object finalPayload = payload;
        final int modelRow = jobModel.getRowCount();
        final long generation = historyGeneration;
        // The row is added first, so a fast result can never land on a missing row.
        jobModel.addRow(new Object[]{"Connecting", op, inputSummary, "Connecting", "", 0});
        rowByJobId.put(requestId, modelRow);
        historyLayout.show(historyContent, "table");
        jobCount.setText(jobModel.getRowCount() + (jobModel.getRowCount() == 1 ? " job" : " jobs"));
        statusLabel.setText("Contacting coordinator...");

        // Run discovery and blocking gRPC calls off the EDT so the window stays responsive.
        new Thread(() -> {
            try {
                connectBackend();
                final int jobId = grpcDirectory.allocateJobId();

                assignedId.set(jobId);
                SwingUtilities.invokeAndWait(() -> {
                    if (generation != historyGeneration) return;
                    rowByJobId.remove(requestId);
                    rowByJobId.put(jobId, modelRow);
                    jobModel.setValueAt(jobId, modelRow, 0);
                });

                // Discover the coordinator and retry submission if needed.
                int coordinatorId = -1;
                // Finding a leader is not acceptance; submitJob() may still throw.
                // Track acceptance so exhausted retries report failure, not "Running".
                boolean accepted = false;
                Exception lastError = null;

                // Three attempts 500 ms apart: one failed submit does not fail the job.
                for (int attempt = 0; attempt < 3; attempt++) {
                    try {
                        WorkerRemote coordinator = discoverCoordinator();

                        if (coordinator == null) {
                            lastError = new RemoteException(
                                    "No coordinator found. Are workers running?");
                            Thread.sleep(500);
                            continue;
                        }

                        coordinatorId = coordinator.getId();

                        com.cs324.registry.proto.WorkerEndpoint jobEndpoint = grpcDirectory.worker(coordinatorId);
                        io.grpc.ManagedChannel jobChannel = io.grpc.ManagedChannelBuilder
                                .forAddress(jobEndpoint.getHost(), jobEndpoint.getGrpcPort()).usePlaintext().build();
                        try {
                            com.cs324.job.proto.JobServiceGrpc.newBlockingStub(jobChannel)
                                    .withDeadlineAfter(10, java.util.concurrent.TimeUnit.SECONDS)
                                    .submitJob(com.cs324.job.proto.SubmitJobRequest.newBuilder().setJobId(jobId)
                                            .setJobType(op).setPayload(com.cs324.computation.ComputationCodec.encode(finalPayload))
                                            .setClient(com.cs324.job.proto.ClientEndpoint.newBuilder().setHost("localhost")
                                                    .setPort(clientPort).setName("Client")).build());
                        } finally { jobChannel.shutdown(); }

                        accepted = true;
                        // Progress failure must never resubmit an already accepted job.
                        try {
                            progressView.watch(jobEndpoint.getHost(), jobEndpoint.getGrpcPort(), jobId, modelRow,
                                    () -> generation == historyGeneration, grpcDirectory::workers);
                        } catch (Exception progressError) {
                            SwingUtilities.invokeLater(() -> {
                                if (generation == historyGeneration) jobModel.setValueAt("Unavailable", modelRow, 5);
                            });
                        }
                        break;

                    } catch (io.grpc.StatusRuntimeException e) {
                        lastError = e;
                        if (e.getStatus().getCode() != io.grpc.Status.Code.FAILED_PRECONDITION) break;
                        Thread.sleep(500);
                    } catch (RemoteException e) {
                        lastError = e;
                        Thread.sleep(500);

                    } catch (Exception e) {
                        lastError = e;
                        break;
                    }
                }

                // Retries exhausted: mark the row Failed, not left as Running.
                if (!accepted) {
                    final String failMessage =
                            lastError != null && lastError.getMessage() != null
                                    ? lastError.getMessage()
                                    : "Unknown submission error";

                    // Back on the event thread, so model and label updates are legal.
                    SwingUtilities.invokeLater(() -> {
                        if (generation == historyGeneration) onSubmissionFailed(jobId, modelRow, failMessage);
                    });

                    return;
                }

                // A fast result can already be queued, so this can overwrite its status.

                SwingUtilities.invokeLater(() -> {
                    if (generation != historyGeneration) return;
                    jobModel.setValueAt(jobId, modelRow, 0);
                    if (!rowByJobId.containsKey(jobId)) return;
                    jobModel.setValueAt(
                            "Processing",
                            modelRow,
                            3
                    );
                    jobModel.setValueAt("Running", modelRow, 4);

                    statusLabel.setText(
                            "Job " + jobId + " submitted to the coordinator."
                    );
                });
            // Anything thrown off the EDT is reported in the row, not just printed.
            } catch (Exception exception) {
                String message = exception.getMessage() == null
                        ? exception.getClass().getSimpleName()
                        : exception.getMessage();
                SwingUtilities.invokeLater(() -> {
                    if (generation == historyGeneration) onSubmissionFailed(assignedId.get(), modelRow, message);
                });
            }
        }).start();
    }

    // Runs on the event thread: turns the connecting placeholder into a reported failure.
    private void onSubmissionFailed(int jobId, int row, String message) {
        rowByJobId.remove(jobId);
        jobModel.setValueAt("Failed", row, 3);
        jobModel.setValueAt(message, row, 4);
        statusLabel.setText("Job " + jobId + " failed: " + message);
        clusterFeed.appendEvents(List.of(new ClusterMonitor.ClusterEvent(java.time.Instant.now(),
                "Job " + jobId + " submission failed: " + message + " [" + jobModel.getValueAt(row, 1) + "]")), autoScroll.isSelected());
    }

    // Updates the matching row once the coordinator returns the aggregated result.
    private void onResult(int jobId, Object result) {
        // Called on the event thread; the callback in connectBackend already hopped.
        Integer row = rowByJobId.get(jobId);
        if (row == null) {
            // Unknown job: its row was never added, or the window already dropped it.
            return;
        }
        if (row >= 0 && row < jobModel.getRowCount()) {
            jobModel.setValueAt(result instanceof Number ? "Completed" : "Failed", row, 3);
            jobModel.setValueAt(String.valueOf(result), row, 4);
        }
        rowByJobId.remove(jobId);
        statusLabel.setText("Job " + jobId + " completed with result: " + result);
    }

    // Displays validation and file-reading errors in a dialog attached to this window.
    private void showError(String message) {
        JOptionPane.showMessageDialog(
                this,
                message,
                "Input error",
                JOptionPane.ERROR_MESSAGE
        );
    }

    public static void main(String[] args) {
        // Optional args: ClientApp [bootstrapHost] [bootstrapPort] [clientRmiPort].
        String bootstrapHost = args.length > 0 ? args[0] : "localhost";
        int bootstrapPort = args.length > 1 ? Integer.parseInt(args[1]) : 1099;
        int clientPort = args.length > 2 ? Integer.parseInt(args[2]) : 1199;
        int grpcPort = args.length > 3 ? Integer.parseInt(args[3]) : com.cs324.common.Constants.DEFAULT_GRPC_PORT;
        // Swing components must be built on the event thread; all RMI work runs elsewhere.
        final String host = bootstrapHost;
        final int bPort = bootstrapPort;
        final int cPort = clientPort;
        SwingUtilities.invokeLater(
                () -> new ClientApp(host, bPort, cPort, grpcPort).setVisible(true)
        );
    }
}
