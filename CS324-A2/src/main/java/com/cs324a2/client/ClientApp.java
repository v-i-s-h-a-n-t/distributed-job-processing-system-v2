package com.cs324a2.client;

import com.cs324a2.common.ComputeOperation;
import com.cs324a2.common.JobRequest;
import com.cs324a2.grpc.coordinator.JobProgressUpdate;
import com.cs324a2.grpc.coordinator.JobStatus;
import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.RenderingHints;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.RoundRectangle2D;
import java.io.File;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;

public class ClientApp extends JFrame {

    private static final Color SURFACE = Color.WHITE;
    private static final Color LINE = new Color(0xE2E8F0);
    private static final Color TEXT = new Color(0x0F172A);
    private static final Color MUTED = new Color(0x64748B);
    private static final Color ACCENT = new Color(0x2563EB);
    private static final Color BTN_PRIMARY = new Color(0x1E293B);
    private static final Color BTN_PRIMARY_HOVER = new Color(0x0F172A);
    private static final Color BTN_PRIMARY_PRESSED = new Color(0x020617);
    private static final Color BTN_PRIMARY_DISABLED = new Color(0x94A3B8);
    private static final Color GREEN = new Color(0x15803D);
    private static final Color RED = new Color(0xB91C1C);
    private static final Color ZEBRA = new Color(0xF8FAFC);
    private static final int PAD = 26;
    private static final int SECTION_GAP = 24;
    private static final int FIELD_H = 38;

    private Font bodyFont;
    private Font boldFont;
    private Font titleFont;
    private Font monoFont;
    private Font sectionFont;
    private Font headerFont;

    private final JTextField hostField = new JTextField("localhost");
    private final JSpinner portField = new JSpinner(new SpinnerNumberModel(50051, 1, 65535, 1));
    private final JTextField clientIdField = new JTextField("client-" + UUID.randomUUID().toString().substring(0, 4));
    private final JComboBox<ComputeOperation> operationBox = new JComboBox<>(ComputeOperation.values());
    private final JTextArea numbersArea = new JTextArea();
    private final JSpinner startField =
            new JSpinner(new LongSpinnerModel(1L, Long.MIN_VALUE, Long.MAX_VALUE, 1L));
    private final JSpinner endField =
            new JSpinner(new LongSpinnerModel(1000L, Long.MIN_VALUE, Long.MAX_VALUE, 1L));

    private final CardLayout inputLayout = new CardLayout();
    private final JPanel inputPanel = new JPanel(inputLayout);
    private final JPanel numbersCard = new JPanel(new BorderLayout());
    private final JPanel rangeCard = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
    private final JLabel inputLabel = new JLabel();
    private final JLabel jobDescriptionLabel = new JLabel();

    /** Column indices in the job history table. */
    static final int COL_STATUS = 3;
    static final int COL_PROGRESS = 4;
    static final int COL_RESULT = 5;

    private final DefaultTableModel tableModel = new DefaultTableModel(
            new Object[]{"Job", "Operation", "Input", "Status", "Progress", "Result"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }

        @Override
        public Class<?> getColumnClass(int column) {
            return column == COL_PROGRESS ? Integer.class : Object.class;
        }
    };
    private final JTable table = new JTable(tableModel);
    /** Latest progress per job, keyed by job ID so cleared rows don't break running jobs. */
    private final Map<String, JobProgressUpdate> progressByJob = new ConcurrentHashMap<>();
    /** Progress stream handles per job. */
    private final Map<String, GrpcClientService.ProgressHandle> progressHandles =
            new ConcurrentHashMap<>();
    /** Jobs whose progress stream already finished. */
    private final java.util.Set<String> finishedProgressStreams =
            ConcurrentHashMap.newKeySet();
    private final java.util.Set<String> progressFetches = ConcurrentHashMap.newKeySet();

    private final JLabel statusLabel = new JLabel("0 workers online");
    private final JLabel runningLabel = new JLabel("0 running");

    private final ExecutorService submitPool = Executors.newFixedThreadPool(8);
    private final AtomicInteger jobCounter = new AtomicInteger(0);
    private final AtomicInteger running = new AtomicInteger(0);
    private final AtomicBoolean workerPollInProgress = new AtomicBoolean(false);
    private Timer workerPollTimer;

    public ClientApp() {
        super("CS324-A2 Client");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        initFonts();
        initComponents();
        buildLayout();

        operationBox.addActionListener(e -> updateInputs());
        updateInputs();

        setMinimumSize(new Dimension(980, 700));
        setSize(1180, 820);
        setLocationRelativeTo(null);
        startWorkerPolling();
    }

    private void initFonts() {
        Font sys = UIManager.getFont("Label.font");
        if (sys == null) {
            sys = new Font(Font.SANS_SERIF, Font.PLAIN, 13);
        }
        bodyFont = sys;
        boldFont = sys.deriveFont(Font.BOLD);
        titleFont = sys.deriveFont(Font.BOLD, sys.getSize2D() + 4);
        sectionFont = sys.deriveFont(Font.BOLD, sys.getSize2D() - 1);
        headerFont = sys.deriveFont(Font.BOLD, sys.getSize2D());
        monoFont = new Font(Font.MONOSPACED, Font.PLAIN, Math.round(sys.getSize2D()));
    }

    private void initComponents() {
        styleField(hostField);
        hostField.setPreferredSize(new Dimension(10, FIELD_H));

        styleField(clientIdField);
        clientIdField.setPreferredSize(new Dimension(10, FIELD_H));

        styleSpinner(portField);
        portField.setPreferredSize(new Dimension(10, FIELD_H));

        styleSpinner(startField);
        startField.setPreferredSize(new Dimension(230, FIELD_H));
        styleSpinner(endField);
        endField.setPreferredSize(new Dimension(230, FIELD_H));

        operationBox.setFont(bodyFont);
        operationBox.setBackground(SURFACE);
        operationBox.setBorder(fieldBorder());
        operationBox.setRenderer(new PaddedComboRenderer());
        operationBox.setPreferredSize(new Dimension(10, FIELD_H));

        numbersArea.setFont(monoFont);
        numbersArea.setLineWrap(true);
        numbersArea.setWrapStyleWord(true);
        numbersArea.setText("2, 4, 5, 11");
        numbersArea.setForeground(TEXT);
        numbersArea.setBorder(BorderFactory.createEmptyBorder(8, 10, 8, 10));
        numbersArea.setBackground(SURFACE);

        JScrollPane numbersScroll = new JScrollPane(numbersArea);
        numbersScroll.setBorder(fieldBorder());
        numbersScroll.getViewport().setBackground(SURFACE);
        numbersScroll.setPreferredSize(new Dimension(520, 130));
        numbersScroll.setMinimumSize(new Dimension(380, 130));
        numbersCard.setBackground(SURFACE);
        numbersCard.add(numbersScroll, BorderLayout.CENTER);

        rangeCard.setBackground(SURFACE);
        rangeCard.add(startField);
        rangeCard.add(new JLabel("to"));
        rangeCard.add(endField);
        rangeCard.setPreferredSize(new Dimension(520, 44));
        rangeCard.setAlignmentX(LEFT_ALIGNMENT);

        inputPanel.setBackground(SURFACE);
        inputPanel.add(numbersCard, "numbers");
        inputPanel.add(rangeCard, "range");
        inputPanel.setPreferredSize(new Dimension(800, 150));

        inputLabel.setFont(boldFont);
        inputLabel.setForeground(TEXT);
        jobDescriptionLabel.setFont(bodyFont);
        jobDescriptionLabel.setForeground(MUTED);

        statusLabel.setFont(bodyFont);
        statusLabel.setForeground(MUTED);
        statusLabel.setText("0 workers online");
        runningLabel.setFont(bodyFont);
        runningLabel.setForeground(MUTED);

        styleTable();
    }

    private void styleField(JTextField field) {
        field.setFont(bodyFont);
        field.setForeground(TEXT);
        field.setBackground(SURFACE);
        field.setBorder(fieldBorder());
        field.setCaretColor(TEXT);
    }

    private void styleSpinner(JSpinner spinner) {
        // "0" format: no thousands separators, exact digits
        JSpinner.NumberEditor editor = new JSpinner.NumberEditor(spinner, "0");
        spinner.setEditor(editor);
        spinner.setBackground(SURFACE);
        spinner.setFont(bodyFont);
        if (spinner.getEditor() instanceof JSpinner.DefaultEditor defaultEditor) {
            JTextField field = defaultEditor.getTextField();
            field.setFont(bodyFont);
            field.setForeground(TEXT);
            field.setBackground(SURFACE);
            field.setBorder(fieldBorder());
            field.setCaretColor(TEXT);
        }
    }

    private void styleTable() {
        table.setFont(bodyFont);
        table.setRowHeight(Math.max(34, bodyFont.getSize() + 18));
        table.setFillsViewportHeight(true);
        table.setShowVerticalLines(false);
        table.setShowHorizontalLines(false);
        table.setIntercellSpacing(new Dimension(0, 1));
        table.setSelectionBackground(new Color(0xDBEAFE));
        table.setSelectionForeground(TEXT);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
        table.setFocusable(false);
        table.setBackground(SURFACE);
        table.setDefaultRenderer(Object.class, new StatusAwareRenderer());

        JTableHeader header = table.getTableHeader();
        header.setFont(headerFont);
        header.setBackground(SURFACE);
        header.setForeground(MUTED);
        header.setReorderingAllowed(false);
        header.setPreferredSize(new Dimension(10, Math.max(32, bodyFont.getSize() + 16)));
        header.setDefaultRenderer(new HeaderRenderer());

        table.getColumnModel().getColumn(0).setPreferredWidth(130);
        table.getColumnModel().getColumn(1).setPreferredWidth(110);
        table.getColumnModel().getColumn(2).setPreferredWidth(280);
        table.getColumnModel().getColumn(COL_STATUS).setPreferredWidth(90);
        table.getColumnModel().getColumn(COL_PROGRESS).setPreferredWidth(150);
        table.getColumnModel().getColumn(COL_RESULT).setPreferredWidth(110);
        table.getColumnModel().getColumn(COL_PROGRESS).setCellRenderer(new ProgressRenderer());
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                int viewRow = table.rowAtPoint(e.getPoint());
                int viewCol = table.columnAtPoint(e.getPoint());
                if (viewRow < 0 || viewCol < 0) return;
                int modelCol = table.convertColumnIndexToModel(viewCol);
                if (modelCol != COL_PROGRESS) return;
                int modelRow = table.convertRowIndexToModel(viewRow);
                showBreakdown(modelRow);
            }
        });
    }

    private Border fieldBorder() {
        return BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(LINE, 1, true),
                BorderFactory.createEmptyBorder(6, 10, 6, 10));
    }

    private void buildLayout() {
        JPanel root = new JPanel(new BorderLayout());
        root.setBackground(SURFACE);
        root.add(buildHeader(), BorderLayout.NORTH);

        JPanel content = new JPanel(new GridBagLayout());
        content.setBackground(SURFACE);
        content.setBorder(BorderFactory.createEmptyBorder(8, PAD, PAD, PAD));

        GridBagConstraints gc = new GridBagConstraints();
        gc.gridx = 0;
        gc.weightx = 1;
        gc.fill = GridBagConstraints.HORIZONTAL;

        gc.gridy = 0;
        gc.weighty = 0;
        gc.insets = new Insets(10, 0, SECTION_GAP, 0);
        content.add(buildConnectionBar(), gc);

        gc.gridy = 1;
        gc.weighty = 0;
        gc.insets = new Insets(0, 0, SECTION_GAP, 0);
        content.add(buildJobSection(), gc);

        gc.gridy = 2;
        gc.weighty = 1;
        gc.fill = GridBagConstraints.BOTH;
        gc.insets = new Insets(0, 0, 0, 0);
        content.add(buildResultsSection(), gc);

        root.add(content, BorderLayout.CENTER);
        setContentPane(root);
    }

    private JPanel buildHeader() {
        JPanel header = new JPanel(new BorderLayout());
        header.setBackground(SURFACE);
        header.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 1, 0, LINE),
                BorderFactory.createEmptyBorder(16, PAD, 16, PAD)));

        JLabel title = new JLabel("CS324-A2 Client");
        title.setFont(titleFont);
        title.setForeground(TEXT);
        JLabel subtitle = new JLabel("Distributed compute workspace");
        subtitle.setFont(bodyFont);
        subtitle.setForeground(MUTED);
        JPanel titleGroup = new JPanel();
        titleGroup.setLayout(new BoxLayout(titleGroup, BoxLayout.Y_AXIS));
        titleGroup.setOpaque(false);
        titleGroup.add(title);
        titleGroup.add(Box.createVerticalStrut(3));
        titleGroup.add(subtitle);

        statusLabel.setHorizontalAlignment(SwingConstants.RIGHT);
        statusLabel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(LINE, 1, true),
                BorderFactory.createEmptyBorder(8, 12, 8, 12)));
        header.add(titleGroup, BorderLayout.WEST);
        header.add(statusLabel, BorderLayout.EAST);
        return header;
    }

    private JPanel buildConnectionBar() {
        JPanel wrap = new JPanel();
        wrap.setLayout(new BoxLayout(wrap, BoxLayout.Y_AXIS));
        wrap.setBackground(SURFACE);

        JLabel head = section("CONNECTION");
        head.setAlignmentX(LEFT_ALIGNMENT);
        wrap.add(head);
        wrap.add(Box.createVerticalStrut(10));

        JPanel bar = new JPanel(new java.awt.GridLayout(1, 3, 12, 0));
        bar.setBackground(SURFACE);
        bar.setAlignmentX(LEFT_ALIGNMENT);
        bar.add(connectionField("Host", hostField));
        bar.add(connectionField("Port", portField));
        bar.add(connectionField("Client ID", clientIdField));
        wrap.add(bar);

        wrap.setBorder(sectionBorder());
        return wrap;
    }

    private JPanel connectionField(String labelText, JComponent field) {
        JPanel col = new JPanel(new BorderLayout(0, 6));
        col.setBackground(SURFACE);
        col.add(boldLabel(labelText), BorderLayout.NORTH);
        col.add(field, BorderLayout.CENTER);
        return col;
    }

    private JPanel buildJobSection() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(SURFACE);

        JLabel head = section("NEW JOB");
        head.setAlignmentX(LEFT_ALIGNMENT);
        panel.add(head);
        panel.add(Box.createVerticalStrut(10));

        JButton load = button("Load CSV", false);
        load.addActionListener(e -> loadCsv());
        JButton submit = button("Submit Job", true);
        submit.addActionListener(e -> submitCurrentJob());

        JPanel top = new JPanel(new java.awt.GridLayout(1, 3, 12, 0));
        top.setBackground(SURFACE);
        top.setAlignmentX(LEFT_ALIGNMENT);
        top.add(connectionField("Operation", operationBox));
        top.add(connectionField(" ", load));
        top.add(connectionField(" ", submit));
        panel.add(top);
        panel.add(Box.createVerticalStrut(12));

        inputLabel.setAlignmentX(LEFT_ALIGNMENT);
        panel.add(inputLabel);
        panel.add(Box.createVerticalStrut(6));
        jobDescriptionLabel.setAlignmentX(LEFT_ALIGNMENT);
        panel.add(jobDescriptionLabel);
        panel.add(Box.createVerticalStrut(8));

        inputPanel.setAlignmentX(LEFT_ALIGNMENT);
        inputPanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 170));
        panel.add(inputPanel);

        panel.setBorder(sectionBorder());
        return panel;
    }

    private JPanel buildResultsSection() {
        JPanel panel = new JPanel(new BorderLayout(0, 10));
        panel.setBackground(SURFACE);

        JPanel head = new JPanel(new BorderLayout());
        head.setOpaque(false);
        head.setBackground(SURFACE);
        head.add(section("JOB HISTORY"), BorderLayout.WEST);
        JPanel east = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        east.setOpaque(false);
        east.setBackground(SURFACE);
        runningLabel.setHorizontalAlignment(SwingConstants.RIGHT);
        east.add(runningLabel);
        JButton clearButton = button("Clear finished", false);
        clearButton.setPreferredSize(new Dimension(fittedWidth(clearButton, "Clear finished"), 30));
        clearButton.setToolTipText("Remove DONE and FAILED jobs from the history");
        clearButton.addActionListener(e -> clearFinishedJobs());
        east.add(clearButton);
        head.add(east, BorderLayout.EAST);
        panel.add(head, BorderLayout.NORTH);

        JScrollPane scroll = new JScrollPane(table);
        scroll.setBorder(BorderFactory.createLineBorder(LINE, 1, true));
        scroll.getViewport().setBackground(SURFACE);
        scroll.setPreferredSize(new Dimension(900, 300));
        panel.add(scroll, BorderLayout.CENTER);
        return panel;
    }

    private JLabel section(String text) {
        JLabel label = new JLabel(text);
        label.setFont(sectionFont);
        label.setForeground(MUTED);
        return label;
    }

    private JLabel boldLabel(String text) {
        JLabel label = new JLabel(text);
        label.setFont(boldFont);
        label.setForeground(TEXT);
        return label;
    }

    private Border sectionBorder() {
        return BorderFactory.createCompoundBorder(
                BorderFactory.createEmptyBorder(14, 4, 20, 4),
                BorderFactory.createMatteBorder(0, 0, 1, 0, LINE));
    }

    private JButton button(String text, boolean primary) {
        return new FlatButton(text, primary);
    }

    /** Button width that fits its text plus insets. */
    private static int fittedWidth(JButton button, String text) {
        java.awt.image.BufferedImage probe =
                new java.awt.image.BufferedImage(1, 1, java.awt.image.BufferedImage.TYPE_INT_ARGB);
        Graphics2D probeGraphics = probe.createGraphics();
        try {
            Insets insets = button.getInsets();
            int textWidth = probeGraphics.getFontMetrics(button.getFont()).stringWidth(text);
            return textWidth + insets.left + insets.right + 8;
        } finally {
            probeGraphics.dispose();
        }
    }

    private void updateInputs() {
        ComputeOperation op = (ComputeOperation) operationBox.getSelectedItem();
        boolean isRange = op == ComputeOperation.PRIMESUM;
        inputLabel.setText(isRange ? "Range" : "Numbers");
        jobDescriptionLabel.setText(switch (op) {
            case MAX -> "Enter integers separated by commas, spaces, or new lines. Returns the largest value.";
            case PRIMECOUNT -> "Enter integers to count. Duplicate prime values are counted separately.";
            case PRIMESUM -> "Choose an inclusive start and end. Large ranges may take a long time.";
        });
        inputLayout.show(inputPanel, isRange ? "range" : "numbers");
    }

    private void startWorkerPolling() {
        refreshWorkerCount();
        workerPollTimer = new Timer(2000, e -> refreshWorkerCount());
        workerPollTimer.setRepeats(true);
        workerPollTimer.start();
    }

    private void refreshWorkerCount() {
        if (!workerPollInProgress.compareAndSet(false, true)) return;
        // read host/port on the EDT; spinner may have uncommitted text
        final String host;
        final int port;
        try {
            host = hostField.getText().trim();
            port = readPort();
        } catch (IllegalArgumentException e) {
            workerPollInProgress.set(false);
            return; // user is mid-edit; retry on the next tick
        }
        try {
            submitPool.submit(() -> {
                try {
                    try (GrpcClientService grpc = new GrpcClientService(host, port)) {
                        int count = grpc.listWorkers().size();
                        SwingUtilities.invokeLater(() -> {
                            statusLabel.setText("●  " + count
                                    + (count == 1 ? " worker online" : " workers online"));
                            statusLabel.setForeground(count > 0 ? GREEN : MUTED);
                            statusLabel.setBackground(count > 0
                                    ? new Color(0xF0FDF4) : new Color(0xF8FAFC));
                            statusLabel.setOpaque(true);
                        });
                    }
                } catch (Exception ex) {
                    SwingUtilities.invokeLater(() -> {
                        statusLabel.setText("●  Disconnected");
                        statusLabel.setForeground(RED);
                        statusLabel.setBackground(new Color(0xFEF2F2));
                        statusLabel.setOpaque(true);
                    });
                } finally {
                    workerPollInProgress.set(false);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            workerPollInProgress.set(false);
        }
    }

    private void updateRunning() {
        int n = running.get();
        int total = tableModel.getRowCount();
        SwingUtilities.invokeLater(() -> runningLabel.setText(
                n + " running  ·  " + total + " jobs"));
    }

    private String grpcHost() {
        return hostField.getText().trim();
    }

    private int grpcPort() {
        return readPort();
    }

    /** Commit the editor and read the port as int. */
    private int readPort() {
        return toPort(commitAndReadLong(portField, "port"));
    }

    /** Raw editor text, or null if no text editor. */
    static String editorText(JSpinner spinner) {
        return (spinner.getEditor() instanceof JSpinner.DefaultEditor defaultEditor)
                ? defaultEditor.getTextField().getText() : null;
    }

    /**
     * Parse the spinner text as a long directly, then commit. NumberEditor's
     * format parser is lenient ("12x" -> 12) and clamps huge values, so we
     * don't trust it alone. Call on the EDT.
     */
    static long commitAndReadLong(JSpinner spinner, String label) {
        String text = editorText(spinner);
        long parsed;
        if (text != null && !text.isBlank()) {
            try {
                parsed = Long.parseLong(text.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(label
                        + " must be a whole number that fits in 64 bits (got '" + text.trim() + "')");
            }
        } else {
            throw new IllegalArgumentException(label + " must not be empty");
        }
        try {
            spinner.commitEdit();
        } catch (ParseException e) {
            throw new IllegalArgumentException(label + " must be a valid integer");
        }
        Object value = spinner.getValue();
        if (!(value instanceof Number number) || number.longValue() != parsed) {
            throw new IllegalArgumentException(label + " must be a whole number"
                    + " that fits in 64 bits (got '" + text.trim() + "')");
        }
        return parsed;
    }

    /** Validate port range, then cast to int. */
    static int toPort(long port) {
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        return (int) port;
    }



    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = chooser.getSelectedFile();
        try {
            if (operationBox.getSelectedItem() == ComputeOperation.PRIMESUM) {
                try {
                    long[] range = JobParser.loadPrimeSumRange(file);
                    startField.setValue(range[0]);
                    endField.setValue(range[1]);
                } catch (java.io.IOException rangeError) {
                    List<Integer> numbers;
                    try {
                        numbers = JobParser.loadNumberList(file);
                    } catch (java.io.IOException listError) {
                        throw rangeError;
                    }
                    Object[] choices = {ComputeOperation.MAX, ComputeOperation.PRIMECOUNT,
                            "Cancel"};
                    int choice = JOptionPane.showOptionDialog(this,
                            file.getName() + " contains a list of numbers. Choose an operation:",
                            "Load number list", JOptionPane.DEFAULT_OPTION,
                            JOptionPane.QUESTION_MESSAGE, null, choices, choices[0]);
                    if (choice < 0 || choice == 2) return;
                    operationBox.setSelectedItem(choices[choice]);
                    numbersArea.setText(numbers.toString().replaceAll("[\\[\\]]", ""));
                }
            } else {
                List<Integer> numbers = JobParser.loadNumberList(file);
                numbersArea.setText(numbers.toString().replaceAll("[\\[\\]]", ""));
            }
        } catch (Exception e) {
            String detail = e.getMessage() == null ? "" : "\n" + e.getMessage();
            JOptionPane.showMessageDialog(this, "Could not read the selected file." + detail,
                    "Load failed", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void submitCurrentJob() {
        ComputeOperation op = (ComputeOperation) operationBox.getSelectedItem();
        String jobId = clientIdField.getText().trim() + "-" + jobCounter.incrementAndGet();
        JobRequest request;
        String inputSummary;
        try {
            if (op == ComputeOperation.PRIMESUM) {
                // read start/end as longs before building the request
                long start = commitAndReadLong(startField, "start");
                long end = commitAndReadLong(endField, "end");
                JobRequest.checkRangeSpan(start, end);
                request = JobRequest.primeSum(jobId, start, end);
                inputSummary = start + "," + end;
            } else if (op == ComputeOperation.MAX) {
                List<Integer> numbers = JobParser.parseNumberList(numbersArea.getText());
                request = JobRequest.max(jobId, numbers);
                inputSummary = summarize(numbers);
            } else {
                List<Integer> numbers = numbersArea.getText().isBlank()
                        ? List.of()
                        : JobParser.parseNumberList(numbersArea.getText());
                request = JobRequest.primeCount(jobId, numbers);
                inputSummary = summarize(numbers);
            }
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(),
                    "Invalid input", JOptionPane.ERROR_MESSAGE);
            return;
        }

        addRow(jobId, op, inputSummary);
        running.incrementAndGet();
        updateRunning();
        String host = grpcHost();
        int port = grpcPort();
        new SwingWorker<String, Void>() {
            @Override
            protected String doInBackground() throws Exception {
                return submitPool.submit(() -> {
                    GrpcClientService grpc = new GrpcClientService(host, port);
                    try {
                        GrpcClientService.ProgressHandle handle =
                                grpc.subscribeProgress(request.jobId(),
                                        new GrpcClientService.ProgressListener() {
                                            @Override
                                            public void onUpdate(JobProgressUpdate update) {
                                                SwingUtilities.invokeLater(
                                                        () -> updateProgress(jobId, update));
                                                if (update.getStatus() != JobStatus.JOB_ACTIVE) {
                                                    // terminal update; drop handle
                                                    finishProgressStream(jobId);
                                                }
                                            }

                                            @Override
                                            public void onError(Throwable error) {
                                                // submit outcome is the source of truth
                                                finishProgressStream(jobId);
                                            }
                                        });
                        // recheck: stream may finish before put() returns
                        progressHandles.put(jobId, handle);
                        if (finishedProgressStreams.remove(jobId)) {
                            releaseStream(jobId);
                        }
                        try {
                            return grpc.submitExact(request);
                        } finally {
                            grpc.close();
                        }
                    } catch (Exception e) {
                        releaseStream(jobId);
                        try {
                            grpc.close();
                        } catch (RuntimeException ignored) {
                        }
                        throw new RuntimeException(e);
                    }
                }).get();
            }

            @Override
            protected void done() {
                running.decrementAndGet();
                updateRunning();
                try {
                    String result = get();
                    updateRow(jobId, "DONE", result);
                } catch (Exception e) {
                    Throwable cause = e;
                    while (cause.getCause() != null) cause = cause.getCause();
                    updateRow(jobId, "FAILED", cause.getMessage() == null
                            ? cause.getClass().getSimpleName() : cause.getMessage());
                }
            }
        }.execute();
    }

    private static String summarize(List<Integer> numbers) {
        if (numbers.size() <= 6) {
            return numbers.toString();
        }
        return numbers.size() + " values " + numbers.subList(0, 3) + "...";
    }

    private void addRow(String jobId, ComputeOperation op, String input) {
        tableModel.addRow(new Object[]{jobId, op, input, "RUNNING", 0, ""});
        progressByJob.remove(jobId);
    }

    /** Find row by job ID, -1 if not found. Call on the EDT. */
    private int findRow(String jobId) {
        for (int row = 0; row < tableModel.getRowCount(); row++) {
            if (jobId.equals(String.valueOf(tableModel.getValueAt(row, 0)))) return row;
        }
        return -1;
    }

    /** Cancel and remove a job's progress stream handle (once). */
    private void releaseStream(String jobId) {
        GrpcClientService.ProgressHandle handle = progressHandles.remove(jobId);
        if (handle != null) {
            try {
                handle.cancel();
            } catch (RuntimeException ignored) {
            }
        }
    }

    private void finishProgressStream(String jobId) {
        GrpcClientService.ProgressHandle handle = progressHandles.remove(jobId);
        if (handle == null) {
            // callback beat subscribeProgress returning the handle
            finishedProgressStreams.add(jobId);
            return;
        }
        finishedProgressStreams.remove(jobId);
        try {
            handle.cancel();
        } catch (RuntimeException ignored) {
        }
    }

    /** Apply one streamed update: percent from completed tasks. */
    void updateProgress(String jobId, JobProgressUpdate update) {
        if (update == null) return;
        int row = findRow(jobId);
        if (row < 0) return; // cleared from history; nothing to paint
        if ("DONE".equals(String.valueOf(tableModel.getValueAt(row, COL_STATUS)))) {
            // job already done; keep snapshot for breakdown, bar stays 100%
            progressByJob.compute(jobId, (id, last) ->
                    progressForFinishedJob(last, update));
            table.repaint();
            return;
        }
        progressByJob.put(jobId, update);
        int percent = (int) Math.round(update.getCompletionFraction() * 100.0);
        percent = Math.max(0, Math.min(100, percent));
        tableModel.setValueAt(percent, row, COL_PROGRESS);
    }

    private void updateRow(String jobId, String status, String result) {
        SwingUtilities.invokeLater(() -> {
            int row = findRow(jobId);
            if (row < 0) return; // cleared from history; nothing to paint
            tableModel.setValueAt(status, row, COL_STATUS);
            tableModel.setValueAt(result, row, COL_RESULT);
            if (!"DONE".equals(status)) {
                return;
            }
            // finished job is 100%; terminal update may have been lost to the race
            tableModel.setValueAt(100, row, COL_PROGRESS);
            progressByJob.compute(jobId, (id, last) ->
                    last == null ? null : asCompleted(last));
            if (!progressByJob.containsKey(jobId)) loadMissingProgress(jobId);
        });
    }

    /** Fetch final task snapshot if the stream missed a short job. */
    private void loadMissingProgress(String jobId) {
        if (!progressFetches.add(jobId)) return;
        String host = grpcHost();
        int port = grpcPort();
        try {
            submitPool.submit(() -> {
                try (GrpcClientService grpc = new GrpcClientService(host, port)) {
                    JobProgressUpdate checkpoint = grpc.fetchJobProgress(jobId);
                    if (checkpoint != null) {
                        SwingUtilities.invokeLater(() -> updateProgress(jobId, checkpoint));
                    }
                } catch (RuntimeException ignored) {
                    // stream may still deliver the checkpoint
                } finally {
                    progressFetches.remove(jobId);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            progressFetches.remove(jobId);
        }
    }

    /**
     * Mark a checkpoint as completed, keeping real task totals. Used when
     * we learn the job finished before the terminal stream update arrived.
     */
    static JobProgressUpdate asCompleted(JobProgressUpdate last) {
        int total = last.getTotalTasks();
        return last.toBuilder()
                .setCompletedTasks(total)
                .setRunningTasks(0)
                .setPendingTasks(0)
                .setCompletionFraction(1.0)
                .setStatus(JobStatus.JOB_COMPLETED)
                .build();
    }

    static JobProgressUpdate progressForFinishedJob(JobProgressUpdate last,
                                                     JobProgressUpdate update) {
        if (last != null && update.getProgressIndex() < last.getProgressIndex()) return last;
        return update.getStatus() == JobStatus.JOB_COMPLETED ? update : asCompleted(update);
    }

    /** Remove DONE/FAILED rows; RUNNING rows and their streams are kept. */
    private void clearFinishedJobs() {
        int removed = removeFinishedRows(tableModel, progressByJob, progressHandles);
        if (removed > 0) updateRunning();
    }

    /**
     * Remove terminal rows and their progress state/stream handles.
     * Headless-friendly for tests. Call on the EDT in production.
     *
     * @return number of rows removed
     */
    static int removeFinishedRows(DefaultTableModel model,
                                  Map<String, JobProgressUpdate> progressByJob,
                                  Map<String, GrpcClientService.ProgressHandle> handles) {
        List<String> finished = new ArrayList<>();
        for (int row = 0; row < model.getRowCount(); row++) {
            if (!"RUNNING".equals(String.valueOf(model.getValueAt(row, COL_STATUS)))) {
                finished.add(String.valueOf(model.getValueAt(row, 0)));
            }
        }
        java.util.Set<String> drop = new java.util.HashSet<>(finished);
        for (String jobId : drop) {
            progressByJob.remove(jobId);
            GrpcClientService.ProgressHandle handle = handles.remove(jobId);
            if (handle != null) {
                try {
                    handle.cancel();
                } catch (RuntimeException ignored) {
                }
            }
        }
        for (int row = model.getRowCount() - 1; row >= 0; row--) {
            if (drop.contains(String.valueOf(model.getValueAt(row, 0)))) model.removeRow(row);
        }
        return finished.size();
    }

    /**
     * Worker-level breakdown for one job. Click the Progress cell to open.
     * Refreshes live while the stream advances.
     */
    private void showBreakdown(int modelRow) {
        if (modelRow < 0 || modelRow >= tableModel.getRowCount()) return;
        String jobId = String.valueOf(tableModel.getValueAt(modelRow, 0));
        JobProgressUpdate snapshot = progressByJob.get(jobId);
        if (snapshot == null) loadMissingProgress(jobId);
        JDialog dialog = new JDialog(this,
                "Job progress — " + jobId, false);
        dialog.setLayout(new BorderLayout(0, 10));
        dialog.getContentPane().setBackground(SURFACE);
        JPanel content = new JPanel(new BorderLayout(0, 10));
        content.setBackground(SURFACE);
        content.setBorder(BorderFactory.createEmptyBorder(14, 16, 16, 16));

        JLabel summary = new JLabel();
        summary.setFont(bodyFont);
        summary.setForeground(TEXT);
        content.add(summary, BorderLayout.NORTH);

        DefaultTableModel workerModel = new DefaultTableModel(
                new Object[]{"Worker", "Total", "Done", "Running", "Pending", "Failed"}, 0) {
            @Override public boolean isCellEditable(int r, int c) { return false; }
        };
        JTable workerTable = new JTable(workerModel);
        workerTable.setFont(bodyFont);
        workerTable.setRowHeight(Math.max(28, bodyFont.getSize() + 12));
        JScrollPane workerScroll = new JScrollPane(workerTable);
        workerScroll.setPreferredSize(new Dimension(520, 130));

        DefaultTableModel taskModel = new DefaultTableModel(
                new Object[]{"Task", "Part", "Status", "Worker", "Attempt", "Detail"}, 0) {
            @Override public boolean isCellEditable(int r, int c) { return false; }
        };
        JTable taskTable = new JTable(taskModel);
        taskTable.setFont(bodyFont);
        taskTable.setRowHeight(Math.max(28, bodyFont.getSize() + 12));
        JScrollPane taskScroll = new JScrollPane(taskTable);
        taskScroll.setPreferredSize(new Dimension(520, 200));

        JPanel tables = new JPanel(new BorderLayout(0, 8));
        tables.setBackground(SURFACE);
        JLabel workerHead = new JLabel("Per-worker breakdown (live from gRPC stream)");
        workerHead.setFont(boldFont);
        workerHead.setForeground(TEXT);
        tables.add(workerHead, BorderLayout.NORTH);
        JPanel center = new JPanel(new BorderLayout(0, 8));
        center.setBackground(SURFACE);
        center.add(workerScroll, BorderLayout.NORTH);
        center.add(taskScroll, BorderLayout.CENTER);
        tables.add(center, BorderLayout.CENTER);
        content.add(tables, BorderLayout.CENTER);

        JLabel hint = new JLabel("Tip: keep this open while the job runs — it refreshes live.");
        hint.setFont(bodyFont);
        hint.setForeground(MUTED);
        content.add(hint, BorderLayout.SOUTH);

        java.util.concurrent.atomic.AtomicReference<JobProgressUpdate> latest =
                new java.util.concurrent.atomic.AtomicReference<>(snapshot);
        java.util.concurrent.atomic.AtomicReference<Timer> refresherRef =
                new java.util.concurrent.atomic.AtomicReference<>();
        Runnable refresh = () -> {
            JobProgressUpdate streamed = progressByJob.get(jobId);
            if (streamed != null) latest.set(streamed);
            JobProgressUpdate cur = latest.get();
            int row = findRow(jobId);
            String status = row < 0 ? "CLEARED"
                    : String.valueOf(tableModel.getValueAt(row, COL_STATUS));
            if (cur == null) {
                summary.setText("Waiting for the first streamed checkpoint… "
                        + "Status: " + status);
                if ("DONE".equals(status)) loadMissingProgress(jobId);
                return;
            }
            int pct = (int) Math.round(cur.getCompletionFraction() * 100.0);
            summary.setText(String.format(
                    "%d/%d tasks complete (%d%%) · running=%d pending=%d failed=%d · status=%s",
                    cur.getCompletedTasks(), cur.getTotalTasks(), pct,
                    cur.getRunningTasks(), cur.getPendingTasks(), cur.getFailedTasks(),
                    cur.getStatus()));
            workerModel.setRowCount(0);
            for (var w : cur.getWorkerBreakdownList()) {
                String worker = w.getWorkerId() < 0 ? "(unassigned)" : String.valueOf(w.getWorkerId());
                workerModel.addRow(new Object[]{worker, w.getTotalTasks(),
                        w.getCompletedTasks(), w.getRunningTasks(),
                        w.getPendingTasks(), w.getFailedTasks()});
            }
            taskModel.setRowCount(0);
            if (cur.hasJobState()) {
                for (var t : cur.getJobState().getTasksList()) {
                    String detail = t.getHasResult() ? (t.getExactResultValue().isEmpty()
                            ? String.valueOf(t.getResultValue()) : t.getExactResultValue())
                            : t.getLastFailure();
                    String worker = t.getAssignedWorkerId() < 0 ? "—"
                            : String.valueOf(t.getAssignedWorkerId());
                    taskModel.addRow(new Object[]{t.getTaskId(), t.getPartitionId(),
                            t.getStatus().name().replace("TASK_", ""), worker,
                            t.getAttempt(), detail});
                }
            }
        };
        refresh.run();
        Timer refresher = new Timer(500, e -> {
            if (!dialog.isVisible()) return;
            refresh.run();
            if (findRow(jobId) < 0) {
                // row was cleared; freeze on last snapshot
                summary.setText(summary.getText() + " · cleared from history");
                Timer t = refresherRef.get();
                if (t != null) t.stop();
            }
        });
        refresherRef.set(refresher);
        refresher.start();
        dialog.addWindowListener(new java.awt.event.WindowAdapter() {
            @Override public void windowClosed(java.awt.event.WindowEvent e) { refresher.stop(); }
            @Override public void windowClosing(java.awt.event.WindowEvent e) { refresher.stop(); }
        });

        dialog.add(content, BorderLayout.CENTER);
        dialog.setMinimumSize(new Dimension(560, 520));
        dialog.pack();
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    @Override
    public void dispose() {
        if (workerPollTimer != null) {
            workerPollTimer.stop();
        }
        for (GrpcClientService.ProgressHandle handle : progressHandles.values()) {
            try {
                handle.cancel();
            } catch (RuntimeException ignored) {
            }
        }
        progressHandles.clear();
        finishedProgressStreams.clear();
        progressFetches.clear();
        submitPool.shutdownNow();
        super.dispose();
    }

    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
        }
        SwingUtilities.invokeLater(() -> new ClientApp().setVisible(true));
    }

    private static final class PaddedComboRenderer extends DefaultListCellRenderer {
        @Override
        public Component getListCellRendererComponent(JList<?> list, Object value, int index,
                boolean isSelected, boolean cellHasFocus) {
            Component comp = super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            if (comp instanceof JLabel label) {
                label.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 0));
            }
            return comp;
        }
    }

    private static final class FlatButton extends JButton {
        private final boolean primary;

        private FlatButton(String text, boolean primary) {
            super(text);
            this.primary = primary;
            setFont(UIManager.getFont("Label.font").deriveFont(Font.BOLD));
            setContentAreaFilled(false);
            setBorderPainted(false);
            setOpaque(false);
            setFocusPainted(false);
            setRolloverEnabled(true);
            setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            setPreferredSize(new Dimension(10, 42));
            setMinimumSize(new Dimension(10, 42));
            setForeground(primary ? Color.WHITE : TEXT);
        }

        @Override
        protected void paintComponent(Graphics g) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            int w = getWidth();
            int h = getHeight();
            Color bg;
            if (primary) {
                bg = !isEnabled() ? BTN_PRIMARY_DISABLED
                        : getModel().isPressed() ? BTN_PRIMARY_PRESSED
                        : getModel().isRollover() ? BTN_PRIMARY_HOVER
                        : BTN_PRIMARY;
            } else {
                bg = !isEnabled() ? SURFACE
                        : getModel().isPressed() ? new Color(0xE2E8F0)
                        : getModel().isRollover() ? new Color(0xF1F5F9)
                        : SURFACE;
            }
            g2.setColor(bg);
            g2.fill(new RoundRectangle2D.Float(0, 0, w, h, 10, 10));
            if (!primary) {
                g2.setColor(LINE);
                g2.setStroke(new BasicStroke(1f));
                g2.draw(new RoundRectangle2D.Float(0.5f, 0.5f, w - 1, h - 1, 10, 10));
            }
            g2.dispose();
            super.paintComponent(g);
        }
    }

    private static final class HeaderRenderer extends JLabel implements TableCellRenderer {
        private HeaderRenderer() {
            setFont(UIManager.getFont("Label.font").deriveFont(Font.BOLD));
            setForeground(MUTED);
            setOpaque(true);
            setBackground(SURFACE);
            setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, LINE));
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean selected,
                boolean focused, int row, int column) {
            setText(value == null ? "" : value.toString());
            return this;
        }
    }

    private static final class StatusAwareRenderer extends DefaultTableCellRenderer {
        private StatusAwareRenderer() {
            setFont(UIManager.getFont("Label.font"));
            setBorder(BorderFactory.createEmptyBorder(0, 12, 0, 12));
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean selected,
                boolean focused, int row, int column) {
            Component comp = super.getTableCellRendererComponent(table, value, selected, focused, row, column);
            if (comp instanceof JLabel label) {
                label.setForeground(TEXT);
                String text = value == null ? "" : value.toString();
                if (!selected) {
                    if ("DONE".equals(text)) {
                        label.setForeground(GREEN);
                    } else if ("FAILED".equals(text)) {
                        label.setForeground(RED);
                    } else if ("RUNNING".equals(text)) {
                        label.setForeground(ACCENT);
                    }
                    if (column == COL_STATUS) {
                        label.setFont(label.getFont().deriveFont(Font.BOLD));
                    }
                    label.setBackground(row % 2 == 0 ? SURFACE : ZEBRA);
                }
                label.setHorizontalAlignment(SwingConstants.LEFT);
            }
            return comp;
        }
    }

    /** Progress bar renderer for the Progress column (0-100). */
    private final class ProgressRenderer extends JProgressBar implements TableCellRenderer {
        private ProgressRenderer() {
            super(0, 100);
            setStringPainted(true);
            setBorderPainted(false);
            setForeground(ACCENT);
            setBackground(new Color(0xE2E8F0));
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
                boolean selected, boolean focused, int row, int column) {
            int modelRow = table.convertRowIndexToModel(row);
            String jobId = String.valueOf(table.getModel().getValueAt(modelRow, 0));
            if (value instanceof Number number) {
                setIndeterminate(false);
                int pct = Math.max(0, Math.min(100, number.intValue()));
                setValue(pct);
                setString(pct + "%");
                JobProgressUpdate last = progressByJob.get(jobId);
                if (last != null) {
                    setToolTipText(String.format(
                            "%d/%d tasks · running=%d pending=%d — click for worker breakdown",
                            last.getCompletedTasks(), last.getTotalTasks(),
                            last.getRunningTasks(), last.getPendingTasks()));
                } else {
                    setToolTipText("Waiting for progress stream — click for worker breakdown");
                }
            } else {
                setIndeterminate(false);
                setValue(0);
                setString("0%");
                setToolTipText("Click for worker breakdown");
            }
            setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            return this;
        }
    }
}
