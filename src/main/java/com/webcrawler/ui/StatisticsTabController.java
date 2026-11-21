package com.webcrawler.ui;

import com.webcrawler.core.CrawlService;
import com.webcrawler.core.StatisticsService;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.HostStatistics;
import com.webcrawler.model.Page;
import com.webcrawler.model.SessionStatus;
import javafx.application.Platform;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.chart.PieChart;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.VBox;

import java.text.NumberFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticsTabController {

    private static final Logger LOGGER = Logger.getLogger(StatisticsTabController.class.getName());
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    private static final Locale UKRAINIAN_LOCALE = new Locale("uk", "UA");
    private static final String ALL_SESSIONS_LABEL = "Усі сесії (загальна статистика)";

    @FXML
    private ComboBox<SessionChoice> sessionSelectorComboBox;
    @FXML
    private Button refreshButton;
    @FXML
    private VBox emptyStateBox;
    @FXML
    private VBox contentContainer;

    @FXML
    private Label visitedKpiLabel;
    @FXML
    private Label matchedKpiLabel;
    @FXML
    private Label errorsKpiLabel;
    @FXML
    private Label totalBytesKpiLabel;
    @FXML
    private Label totalBytesHumanLabel;
    @FXML
    private Label cumulativeSummaryLabel;

    @FXML
    private PieChart hostPieChart;
    @FXML
    private TableView<HostStatistics> hostTable;
    @FXML
    private TableColumn<HostStatistics, String> hostColumn;
    @FXML
    private TableColumn<HostStatistics, Number> hostVisitedColumn;
    @FXML
    private TableColumn<HostStatistics, Number> hostMatchedColumn;
    @FXML
    private TableColumn<HostStatistics, Number> hostErrorColumn;
    @FXML
    private TableColumn<HostStatistics, Number> hostBytesColumn;

    @FXML
    private Label pagesCountLabel;
    @FXML
    private TableView<Page> pagesTable;
    @FXML
    private TableColumn<Page, String> pageUrlColumn;
    @FXML
    private TableColumn<Page, String> pageTitleColumn;
    @FXML
    private TableColumn<Page, String> pageDateColumn;
    @FXML
    private TableColumn<Page, Number> pageSizeColumn;
    @FXML
    private TableColumn<Page, Number> pageDepthColumn;
    @FXML
    private TableColumn<Page, Number> pageStatusColumn;
    @FXML
    private TableColumn<Page, String> pageHasTermColumn;
    @FXML
    private TableColumn<Page, String> pageErrorColumn;
    @FXML
    private TableColumn<Page, String> pageFilePathColumn;

    private final ExecutorService queryExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "ui-statistics-query");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicLong refreshSequence = new AtomicLong(0);

    private CrawlService crawlService;
    private boolean updatingSelector;

    @FXML
    private void initialize() {
        sessionSelectorComboBox.getItems().setAll(new SessionChoice(null, ALL_SESSIONS_LABEL));
        sessionSelectorComboBox.getSelectionModel().selectFirst();
        sessionSelectorComboBox.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (!updatingSelector && crawlService != null) {
                refresh();
            }
        });

        hostPieChart.setAnimated(false);
        configureHostTable();
        configurePagesTable();
        setEmptyStateVisible(true);
    }

    public void setCrawlService(CrawlService crawlService) {
        this.crawlService = Objects.requireNonNull(crawlService, "crawlService must not be null");
        refresh();
    }

    public void refresh() {
        if (crawlService == null) {
            return;
        }
        long requestVersion = refreshSequence.incrementAndGet();
        SessionChoice currentChoice = sessionSelectorComboBox.getValue();
        Long requestedSessionId = currentChoice != null ? currentChoice.sessionId() : null;

        queryExecutor.submit(() -> {
            try {
                boolean hasAnySessions = crawlService.hasAnySessions();
                if (!hasAnySessions) {
                    Platform.runLater(() -> applyEmptyState(requestVersion));
                    return;
                }

                List<CrawlSession> sessions = crawlService.findAllSessions();
                Long effectiveSessionId = isSessionPresent(sessions, requestedSessionId)
                        ? requestedSessionId
                        : null;

                Map<String, Long> overallSummary = crawlService.getOverallSummary();
                Map<String, Long> selectedSummary = effectiveSessionId == null
                        ? overallSummary
                        : crawlService.getSessionSummary(effectiveSessionId);
                Map<String, Long> distribution = effectiveSessionId == null
                        ? crawlService.getHostDistribution()
                        : crawlService.getHostDistribution(effectiveSessionId);
                List<HostStatistics> hostMetrics = crawlService.getHostMetrics(effectiveSessionId);

                List<Page> pages = loadPagesForScope(sessions, effectiveSessionId);
                String comparisonBanner = buildComparisonBanner(sessions, effectiveSessionId, overallSummary);

                Platform.runLater(() -> applyStatisticsSnapshot(
                        requestVersion,
                        sessions,
                        effectiveSessionId,
                        selectedSummary,
                        comparisonBanner,
                        distribution,
                        hostMetrics,
                        pages
                ));
            } catch (RuntimeException ex) {
                LOGGER.log(Level.WARNING, "Failed to load statistics from database", ex);
            }
        });
    }

    public void shutdown() {
        queryExecutor.shutdownNow();
    }

    @FXML
    private void onRefreshClicked() {
        refresh();
    }

    public ComboBox<SessionChoice> getSessionSelectorComboBox() {
        return sessionSelectorComboBox;
    }

    public Button getRefreshButton() {
        return refreshButton;
    }

    public VBox getEmptyStateBox() {
        return emptyStateBox;
    }

    public VBox getContentContainer() {
        return contentContainer;
    }

    private List<Page> loadPagesForScope(List<CrawlSession> sessions, Long sessionId) {
        if (sessionId != null) {
            return crawlService.findPagesBySessionId(sessionId);
        }
        List<Page> allPages = new ArrayList<>();
        for (CrawlSession session : sessions) {
            allPages.addAll(crawlService.findPagesBySessionId(session.getId()));
        }
        return allPages;
    }

    private String buildComparisonBanner(
            List<CrawlSession> sessions,
            Long effectiveSessionId,
            Map<String, Long> overallSummary) {
        NumberFormat fmt = NumberFormat.getIntegerInstance(UKRAINIAN_LOCALE);
        if (effectiveSessionId != null) {
            long visited = overallSummary.getOrDefault(StatisticsService.KEY_VISITED, 0L);
            long matched = overallSummary.getOrDefault(StatisticsService.KEY_MATCHED, 0L);
            long errors = overallSummary.getOrDefault(StatisticsService.KEY_ERRORS, 0L);
            long bytes = overallSummary.getOrDefault(StatisticsService.KEY_TOTAL_BYTES, 0L);
            return "Накопичувально за всіма сесіями (" + sessions.size() + "): відвідано "
                    + fmt.format(visited) + ", збігів " + fmt.format(matched)
                    + ", помилок " + fmt.format(errors) + ", обсяг " + formatBytesHuman(bytes);
        }

        CrawlSession activeOrLatest = crawlService.getActiveSession();
        if (activeOrLatest == null && !sessions.isEmpty()) {
            activeOrLatest = sessions.get(0);
        }
        if (activeOrLatest != null && activeOrLatest.getId() > 0) {
            Map<String, Long> latestSummary = crawlService.getSessionSummary(activeOrLatest.getId());
            long visited = latestSummary.getOrDefault(StatisticsService.KEY_VISITED, 0L);
            long matched = latestSummary.getOrDefault(StatisticsService.KEY_MATCHED, 0L);
            long errors = latestSummary.getOrDefault(StatisticsService.KEY_ERRORS, 0L);
            return "Поточна / остання сесія #" + activeOrLatest.getId()
                    + " (" + translateStatus(activeOrLatest.getStatus()) + "): відвідано "
                    + fmt.format(visited) + ", збігів " + fmt.format(matched)
                    + ", помилок " + fmt.format(errors);
        }
        return "Накопичувальна статистика за всіма сесіями (" + sessions.size() + ").";
    }

    private void applyEmptyState(long requestVersion) {
        if (requestVersion != refreshSequence.get()) {
            return;
        }
        updatingSelector = true;
        try {
            SessionChoice allItem = new SessionChoice(null, ALL_SESSIONS_LABEL);
            sessionSelectorComboBox.getItems().setAll(allItem);
            sessionSelectorComboBox.setValue(allItem);
        } finally {
            updatingSelector = false;
        }
        hostPieChart.getData().clear();
        hostTable.getItems().clear();
        pagesTable.getItems().clear();
        setEmptyStateVisible(true);
    }

    private void applyStatisticsSnapshot(
            long requestVersion,
            List<CrawlSession> sessions,
            Long effectiveSessionId,
            Map<String, Long> summary,
            String comparisonBanner,
            Map<String, Long> distribution,
            List<HostStatistics> hostMetrics,
            List<Page> pages) {
        if (requestVersion != refreshSequence.get()) {
            return;
        }

        setEmptyStateVisible(false);
        updateSessionChoices(sessions, effectiveSessionId);

        NumberFormat fmt = NumberFormat.getIntegerInstance(UKRAINIAN_LOCALE);
        long visited = summary.getOrDefault(StatisticsService.KEY_VISITED, 0L);
        long matched = summary.getOrDefault(StatisticsService.KEY_MATCHED, 0L);
        long errors = summary.getOrDefault(StatisticsService.KEY_ERRORS, 0L);
        long totalBytes = summary.getOrDefault(StatisticsService.KEY_TOTAL_BYTES, 0L);

        visitedKpiLabel.setText(fmt.format(visited));
        matchedKpiLabel.setText(fmt.format(matched));
        errorsKpiLabel.setText(fmt.format(errors));
        totalBytesKpiLabel.setText(fmt.format(totalBytes));
        totalBytesHumanLabel.setText("(" + formatBytesHuman(totalBytes) + ")");
        cumulativeSummaryLabel.setText(comparisonBanner);

        ObservableList<PieChart.Data> pieData = FXCollections.observableArrayList();
        for (Map.Entry<String, Long> entry : distribution.entrySet()) {
            if (entry.getValue() != null && entry.getValue() > 0) {
                pieData.add(new PieChart.Data(entry.getKey() + " (" + entry.getValue() + ")", entry.getValue()));
            }
        }
        hostPieChart.setData(pieData);

        hostTable.getItems().setAll(hostMetrics);
        pagesTable.getItems().setAll(pages);
        pagesCountLabel.setText(pages.size() + " записів");
    }

    private void updateSessionChoices(List<CrawlSession> sessions, Long selectedSessionId) {
        List<SessionChoice> choices = new ArrayList<>();
        SessionChoice allChoice = new SessionChoice(null, ALL_SESSIONS_LABEL);
        choices.add(allChoice);

        SessionChoice targetToSelect = allChoice;
        for (CrawlSession session : sessions) {
            String label = "#" + session.getId()
                    + " — " + session.getSettings().getStartUrl()
                    + " [" + translateStatus(session.getStatus()) + "]"
                    + " (відвідано: " + session.getVisitedCount()
                    + ", збігів: " + session.getMatchedCount() + ")";
            SessionChoice choice = new SessionChoice(session.getId(), label);
            choices.add(choice);
            if (selectedSessionId != null && selectedSessionId.equals(session.getId())) {
                targetToSelect = choice;
            }
        }

        updatingSelector = true;
        try {
            sessionSelectorComboBox.getItems().setAll(choices);
            sessionSelectorComboBox.setValue(targetToSelect);
        } finally {
            updatingSelector = false;
        }
    }

    private void setEmptyStateVisible(boolean empty) {
        emptyStateBox.setVisible(empty);
        emptyStateBox.setManaged(empty);
        contentContainer.setVisible(!empty);
        contentContainer.setManaged(!empty);
    }

    private void configureHostTable() {
        hostTable.setPlaceholder(new Label("Немає статистики за хостами"));
        hostColumn.setCellValueFactory(cell -> new SimpleStringProperty(cell.getValue().host()));
        hostVisitedColumn.setCellValueFactory(cell -> new SimpleLongProperty(cell.getValue().visitedCount()));
        hostMatchedColumn.setCellValueFactory(cell -> new SimpleLongProperty(cell.getValue().matchedCount()));
        hostErrorColumn.setCellValueFactory(cell -> new SimpleLongProperty(cell.getValue().errorCount()));
        hostBytesColumn.setCellValueFactory(cell -> new SimpleLongProperty(cell.getValue().totalBytes()));
    }

    private void configurePagesTable() {
        pagesTable.setPlaceholder(new Label("Немає відвіданих сторінок"));
        pageUrlColumn.setCellValueFactory(cell -> new SimpleStringProperty(safeText(cell.getValue().getUrl(), "")));
        pageTitleColumn.setCellValueFactory(cell -> new SimpleStringProperty(safeText(cell.getValue().getTitle(), "—")));
        pageDateColumn.setCellValueFactory(cell -> {
            var dt = cell.getValue().getDownloadedAt();
            return new SimpleStringProperty(dt != null ? DATE_FORMATTER.format(dt) : "—");
        });
        pageSizeColumn.setCellValueFactory(cell -> new SimpleLongProperty(cell.getValue().getSize()));
        pageDepthColumn.setCellValueFactory(cell -> new SimpleIntegerProperty(cell.getValue().getDepth()));
        pageStatusColumn.setCellValueFactory(cell -> new SimpleIntegerProperty(cell.getValue().getHttpStatus()));
        pageHasTermColumn.setCellValueFactory(cell ->
                new SimpleStringProperty(cell.getValue().isHasTerm() ? "Так" : "Ні"));
        pageErrorColumn.setCellValueFactory(cell -> new SimpleStringProperty(safeText(cell.getValue().getError(), "—")));
        pageFilePathColumn.setCellValueFactory(cell ->
                new SimpleStringProperty(safeText(cell.getValue().getFilePath(), "—")));
    }

    private static boolean isSessionPresent(List<CrawlSession> sessions, Long sessionId) {
        if (sessionId == null) {
            return false;
        }
        return sessions.stream().anyMatch(s -> s.getId() == sessionId);
    }

    private static String translateStatus(SessionStatus status) {
        if (status == null) {
            return "Невідомо";
        }
        return switch (status) {
            case IDLE -> "Очікування";
            case RUNNING -> "Виконується";
            case PAUSED -> "Призупинено";
            case FINISHED -> "Завершено";
            case FAILED -> "Помилка";
        };
    }

    private static String formatBytesHuman(long bytes) {
        if (bytes < 1024) {
            return bytes + " Б";
        }
        double kb = bytes / 1024.0;
        if (kb < 1024) {
            return String.format(UKRAINIAN_LOCALE, "%.1f КБ", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024) {
            return String.format(UKRAINIAN_LOCALE, "%.2f МБ", mb);
        }
        double gb = mb / 1024.0;
        return String.format(UKRAINIAN_LOCALE, "%.2f ГБ", gb);
    }

    private static String safeText(String value, String fallback) {
        return (value == null || value.isBlank()) ? fallback : value;
    }

    public record SessionChoice(Long sessionId, String displayLabel) {
        @Override
        public String toString() {
            return displayLabel;
        }
    }
}
