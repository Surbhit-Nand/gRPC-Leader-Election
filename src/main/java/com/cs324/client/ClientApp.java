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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static com.cs324.client.ClientStyle.*;

// Swing client: prepares input, submits to the current coordinator, then shows the result.
// Arguments are optional: ClientApp [bootstrapHost] [bootstrapPort] [clientRmiPort].
public class ClientApp extends JFrame {

    // Colours, rounded panels and icons come from ClientStyle via the static import above.
    // Bootstrap endpoint, plus the RMI port the coordinator calls our results back on.
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final int clientPort;

    // Keep controls as fields so button handlers can read input and update messages.
    private final JComboBox<String> jobTypeBox =
            new JComboBox<>(new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"});

    private final InputArea inputArea = new InputArea();
    private final JLabel exampleLabel = new JLabel();
    private final JLabel statusLabel =
            new JLabel("Ready to submit jobs");

    // Backend references (lazy: resolved on the first submission).
    private BootstrapRemote bootstrap;
    private ClientRemoteImpl clientRemote;
    // Row index in the model of each in-flight job this window owns, keyed by job id.
    private final ConcurrentHashMap<Integer, Integer> rowByJobId = new ConcurrentHashMap<>();
    private final AtomicInteger nextRequestId = new AtomicInteger(1);

    // The model stores submitted jobs; JTable displays them. Users cannot edit the rows.
    private final DefaultTableModel jobModel = new DefaultTableModel(
            new String[]{"Job ID", "Operation", "Input", "Status", "Result"},
            0
    ) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };

    private final JLabel jobCount = new JLabel("0 jobs");
    // CardLayout displays either the empty-state message or the populated table.
    private final CardLayout historyLayout = new CardLayout();
    private final JPanel historyContent = new JPanel(historyLayout);

    // One-pass construction: builds the three sections, then wires controls to their handlers.
    public ClientApp(String bootstrapHost, int bootstrapPort, int clientPort) {
        super("CS324 Distributed Job Client");
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.clientPort = clientPort;

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(900, 780));

        // Outer layout: heading at the top, workspace in the centre, status at the bottom.
        JPanel root = new JPanel(new BorderLayout(0, 20));
        root.setBackground(BACKGROUND);
        root.setBorder(BorderFactory.createEmptyBorder(24, 28, 16, 28));
        setContentPane(root);

        // The heading includes a breadcrumb and decorative server graphic.
        JPanel header = ClientStyle.header();
        header.setBorder(BorderFactory.createEmptyBorder(0, 20, 6, 20));
        JPanel heading = transparentPanel(null);
        // BoxLayout stacks breadcrumb, title and subtitle; struts give the vertical gaps.
        heading.setLayout(new BoxLayout(heading, BoxLayout.Y_AXIS));
        JLabel breadcrumb = label("CS324   /   Distributed computing", 13, Font.PLAIN, MUTED);
        breadcrumb.setIcon(icon("home", 18, MUTED));
        breadcrumb.setIconTextGap(10);
        heading.add(breadcrumb);
        heading.add(Box.createVerticalStrut(14));
        heading.add(label("Job workspace", 30, Font.BOLD, INK));
        heading.add(Box.createVerticalStrut(5));
        heading.add(label("Prepare your data and review computational jobs.", 16, Font.PLAIN, MUTED));
        header.add(heading, BorderLayout.CENTER);
        root.add(header, BorderLayout.NORTH);

        // Rounded cards group the form and submitted job history.
        JPanel workspace = new Workspace();
        JPanel composer = card(new BorderLayout(0, 20));
        composer.add(sectionHeading("New job", "Choose an operation and provide input data.", "document"),
                BorderLayout.NORTH);

        // Operation selection determines the example text and validation rules.
        JPanel fields = transparentPanel(new BorderLayout(0, 16));
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
        jobTypeBox.setPreferredSize(new Dimension(240, 44));
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
        inputScroll.setPreferredSize(new Dimension(700, 100));
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
        JPanel history = card(new BorderLayout(0, 14));
        JPanel historyHeading = transparentPanel(new BorderLayout());
        historyHeading.add(sectionHeading("Job history",
                "Track submitted jobs and their final results.", "list"), BorderLayout.CENTER);
        jobCount.setFont(BODY.deriveFont(12f));
        jobCount.setForeground(MUTED);
        historyHeading.add(jobCount, BorderLayout.EAST);
        history.add(historyHeading, BorderLayout.NORTH);

        // Sorting changes the displayed order, while the model remains the source of row data.
        JTable jobTable = new JTable(jobModel);
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
        jobTable.getTableHeader().setPreferredSize(new Dimension(0, 38));

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
        int[] widths = {115, 115, 130, 215, 145};
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
        historyContent.setPreferredSize(new Dimension(750, 120));
        historyContent.add(emptyState, "empty");
        historyContent.add(resultsPane, "table");
        historyLayout.show(historyContent, "empty");
        history.add(historyContent, BorderLayout.CENTER);
        workspace.add(history, BorderLayout.CENTER);
        // Scrolling wrapper keeps the cards legible on windows shorter than the content.
        JScrollPane workspaceScroll = new JScrollPane(workspace,
                JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        workspaceScroll.setBorder(BorderFactory.createEmptyBorder());
        workspaceScroll.getViewport().setBackground(BACKGROUND);
        workspaceScroll.getVerticalScrollBar().setUnitIncrement(20);
        root.add(workspaceScroll, BorderLayout.CENTER);
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
        setSize(1120, 870);
        setLocationRelativeTo(null);
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
        text.add(label(title, 20, Font.BOLD, INK));
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
        if (clientRemote == null) {
            Registry clientRegistry = LocateRegistry.createRegistry(clientPort);
            // RMI threads hand results back, so the callback hops onto the event thread.
            clientRemote = new ClientRemoteImpl((jobId, result) ->
                    SwingUtilities.invokeLater(() -> onResult(jobId, result)));
            clientRegistry.rebind("Client", clientRemote);
        }
        if (bootstrap == null) {
            Registry bootstrapRegistry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            bootstrap = (BootstrapRemote) bootstrapRegistry.lookup("BootstrapRegistry");
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
            Registry registry = LocateRegistry.getRegistry(address.getRmiHost(), address.getRmiPort());
            return (WorkerRemote) registry.lookup("Worker" + workerId);
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

        String inputSummary = "PRIMESUM".equals(operation)
                ? values[0] + " to " + values[1]
                : values.length + " numbers";
        // Use a local reference while bootstrap allocates a unique cluster job ID.
        int requestId = -nextRequestId.getAndIncrement();
        AtomicInteger assignedId = new AtomicInteger(requestId);
        final String op = operation;
        final Object finalPayload = payload;
        final int modelRow = jobModel.getRowCount();
        // The row is added first, so a fast result can never land on a missing row.
        jobModel.addRow(new Object[]{"Connecting", op, inputSummary, "Connecting", ""});
        rowByJobId.put(requestId, modelRow);
        historyLayout.show(historyContent, "table");
        jobCount.setText(jobModel.getRowCount() + (jobModel.getRowCount() == 1 ? " job" : " jobs"));
        statusLabel.setText("Contacting coordinator...");

        // Blocking RMI calls run here, never on the EDT, so the window stays responsive.
        new Thread(() -> {
            try {
                connectBackend();
                final int jobId = bootstrap.allocateJobId();

                assignedId.set(jobId);
                rowByJobId.remove(requestId);
                rowByJobId.put(jobId, modelRow);

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

                        coordinator.submitJob(
                                jobId,
                                op,
                                finalPayload,
                                "localhost",
                                clientPort,
                                "Client"
                        );

                        accepted = true;
                        break;

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
                    SwingUtilities.invokeLater(() ->
                            onSubmissionFailed(jobId, modelRow, failMessage)
                    );

                    return;
                }

                final int submittedCoordinatorId = coordinatorId;
                // A fast result can already be queued, so this can overwrite its status.

                SwingUtilities.invokeLater(() -> {
                    jobModel.setValueAt(jobId, modelRow, 0);
                    jobModel.setValueAt(
                            "Submitted to coordinator " + submittedCoordinatorId,
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
                SwingUtilities.invokeLater(() -> onSubmissionFailed(assignedId.get(), modelRow, message));
            }
        }).start();
    }

    // Runs on the event thread: turns the connecting placeholder into a reported failure.
    private void onSubmissionFailed(int jobId, int row, String message) {
        rowByJobId.remove(jobId);
        jobModel.setValueAt("Failed", row, 3);
        jobModel.setValueAt(message, row, 4);
        statusLabel.setText("Job " + jobId + " failed: " + message);
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
        // Swing components must be built on the event thread; all RMI work runs elsewhere.
        final String host = bootstrapHost;
        final int bPort = bootstrapPort;
        final int cPort = clientPort;
        SwingUtilities.invokeLater(
                () -> new ClientApp(host, bPort, cPort).setVisible(true)
        );
    }
}
