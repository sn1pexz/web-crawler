package com.webcrawler.ui;

import com.webcrawler.core.CrawlService;
import com.webcrawler.filter.FilterType;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.Site;
import com.webcrawler.model.StrategyType;
import com.webcrawler.p2p.PeerNode;
import com.webcrawler.storage.DirectoryElement;
import com.webcrawler.storage.PageElement;
import com.webcrawler.storage.SiteElement;
import javafx.application.Platform;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CrawlTabController {

    private static final Logger LOGGER = Logger.getLogger(CrawlTabController.class.getName());
    private static final DateTimeFormatter LOG_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    static final int MIN_P2P_PORT = 1024;
    static final int MAX_P2P_PORT = 65535;
    private static final int MAX_LOG_ENTRIES = 400;
    private static final long TREE_REFRESH_THROTTLE_NANOS = 400_000_000L;

    @FXML
    private TextField startUrlField;
    @FXML
    private TextField searchTermField;
    @FXML
    private ComboBox<StrategyOption> strategyComboBox;
    @FXML
    private Spinner<Integer> maxDepthSpinner;
    @FXML
    private Spinner<Integer> pageLimitSpinner;
    @FXML
    private CheckBox wholeWordCheckBox;
    @FXML
    private CheckBox includeSubdomainsCheckBox;
    @FXML
    private CheckBox respectRobotsCheckBox;
    @FXML
    private CheckBox scriptFilterCheckBox;
    @FXML
    private CheckBox styleFilterCheckBox;
    @FXML
    private CheckBox embeddedFilterCheckBox;
    @FXML
    private CheckBox adFilterCheckBox;
    @FXML
    private CheckBox navigationFilterCheckBox;

    @FXML
    private Button startButton;
    @FXML
    private Button pauseButton;
    @FXML
    private Button resumeButton;
    @FXML
    private Button stopButton;
    @FXML
    private ComboBox<PausedSessionItem> pausedSessionsComboBox;
    @FXML
    private Button refreshPausedButton;
    @FXML
    private Spinner<Integer> cacheTtlSpinner;
    @FXML
    private Button clearCacheButton;

    @FXML
    private TextField initiatorPortField;
    @FXML
    private Button createDistributedButton;
    @FXML
    private Button resumeDistributedButton;
    @FXML
    private TextField workerHostField;
    @FXML
    private TextField workerPortField;
    @FXML
    private Button connectPeerButton;
    @FXML
    private Button disconnectPeerButton;
    @FXML
    private Label workersCountLabel;
    @FXML
    private TableView<PeerNode> peersTable;
    @FXML
    private TableColumn<PeerNode, String> peerIdColumn;
    @FXML
    private TableColumn<PeerNode, String> peerHostColumn;
    @FXML
    private TableColumn<PeerNode, Number> peerPortColumn;
    @FXML
    private TableColumn<PeerNode, String> peerHeartbeatColumn;

    @FXML
    private Label visitedCountLabel;
    @FXML
    private Label matchedCountLabel;
    @FXML
    private Label errorCountLabel;
    @FXML
    private Label sessionStatusLabel;
    @FXML
    private Label treeSummaryLabel;
    @FXML
    private TreeView<String> siteTreeView;
    @FXML
    private Button clearLogButton;
    @FXML
    private ListView<String> eventLogListView;

    private final Map<FilterType, CheckBox> filterCheckBoxes = new EnumMap<>(FilterType.class);
    private final ExecutorService actionExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "ui-crawl-actions");
        thread.setDaemon(true);
        return thread;
    });

    private CrawlService crawlService;
    private boolean actionInProgress;
    private boolean updatingPausedList;
    private int peerVisitedCount;
    private int peerMatchedCount;
    private int peerErrorCount;
    private long lastTreeRefreshNanos;

    @FXML
    private void initialize() {
        strategyComboBox.getItems().setAll(
                new StrategyOption(StrategyType.BFS, "BFS — у ширину"),
                new StrategyOption(StrategyType.DFS, "DFS — у глибину")
        );
        strategyComboBox.getSelectionModel().selectFirst();

        configureIntSpinner(maxDepthSpinner, 0, 100, CrawlSettings.DEFAULT_MAX_DEPTH);
        configureIntSpinner(pageLimitSpinner, 1, 100_000, CrawlSettings.DEFAULT_PAGE_LIMIT);
        configureIntSpinner(cacheTtlSpinner, 0, 8760, 24);

        cacheTtlSpinner.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (crawlService != null && newVal != null && newVal >= 0) {
                crawlService.setCacheTtlHours(newVal);
            }
        });

        filterCheckBoxes.put(FilterType.SCRIPT, scriptFilterCheckBox);
        filterCheckBoxes.put(FilterType.STYLE, styleFilterCheckBox);
        filterCheckBoxes.put(FilterType.EMBEDDED, embeddedFilterCheckBox);
        filterCheckBoxes.put(FilterType.AD, adFilterCheckBox);
        filterCheckBoxes.put(FilterType.NAVIGATION, navigationFilterCheckBox);

        Set<FilterType> defaults = FilterType.defaultEnabled();
        for (Map.Entry<FilterType, CheckBox> entry : filterCheckBoxes.entrySet()) {
            entry.getValue().setSelected(defaults.contains(entry.getKey()));
        }

        if (respectRobotsCheckBox != null) {
            respectRobotsCheckBox.setSelected(true);
            respectRobotsCheckBox.setDisable(true);
        }

        pausedSessionsComboBox.setPlaceholder(new Label("Немає призупинених сесій"));
        pausedSessionsComboBox.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (!updatingPausedList) {
                updateUiState();
            }
        });

        configurePeersTable();
        resetSiteTree();
        eventLogListView.setPlaceholder(new Label("Журнал подій порожній"));
        updateUiState();
    }

    public void setCrawlService(CrawlService crawlService) {
        this.crawlService = Objects.requireNonNull(crawlService, "crawlService must not be null");
        runOnFxThread(() -> {
            cacheTtlSpinner.getValueFactory().setValue(crawlService.getCacheTtlHours());
            refreshPeersDisplay();
            updateUiState();
            refreshPausedSessionsAsync();
        });
    }

    public void shutdown() {
        actionExecutor.shutdownNow();
    }

    @FXML
    private void onStartCrawl() {
        if (!ensureServiceAvailable()) {
            return;
        }
        CrawlSettings settings;
        int cacheTtlHours;
        try {
            settings = buildAndValidateSettings();
            cacheTtlHours = readSpinnerInt(cacheTtlSpinner, "Термін дії кешу (год)", 0, 8760);
        } catch (IllegalArgumentException ex) {
            showErrorAlert("Помилка параметрів обходу", ex.getMessage());
            appendLog("[ПОМИЛКА] " + ex.getMessage());
            return;
        }

        resetCountersForNewRun("Запуск локального обходу...");
        runAsyncAction("Запуск локального обходу", () -> {
            crawlService.setCacheTtlHours(cacheTtlHours);
            CrawlSession session = crawlService.start(settings);
            Platform.runLater(() -> {
                updateCountersFromSession(session);
                refreshSiteTree();
            });
        }, true);
    }

    @FXML
    private void onPauseCrawl() {
        if (!ensureServiceAvailable()) {
            return;
        }
        appendLog("Запит на призупинення поточної сесії...");
        runAsyncAction("Призупинення обходу", () -> crawlService.pause(), true);
    }

    @FXML
    private void onResumeCrawl() {
        if (!ensureServiceAvailable()) {
            return;
        }
        int cacheTtlHours;
        try {
            cacheTtlHours = readSpinnerInt(cacheTtlSpinner, "Термін дії кешу (год)", 0, 8760);
        } catch (IllegalArgumentException ex) {
            showErrorAlert("Помилка налаштувань кешу", ex.getMessage());
            return;
        }

        CrawlSession active = crawlService.getActiveSession();
        if (active != null && active.getStatus() == SessionStatus.PAUSED && !crawlService.isConnectedPeer()) {
            if (active.isDistributed() && crawlService.getSessionCoordinator() == null) {
                showInfoAlert(
                        "Відновлення розподіленої сесії",
                        "Обрана сесія #" + active.getId() + " є розподіленою. "
                                + "Вкажіть порт і натисніть «Відновити розподілену сесію»."
                );
                return;
            }
            runAsyncAction("Відновлення сесії", () -> {
                crawlService.setCacheTtlHours(cacheTtlHours);
                crawlService.resume();
                Platform.runLater(() -> {
                    CrawlSession current = crawlService.getActiveSession();
                    if (current != null) {
                        updateCountersFromSession(current);
                    }
                    refreshPausedSessionsAsync();
                });
            }, true);
            return;
        }

        PausedSessionItem selected = pausedSessionsComboBox.getValue();
        if (selected == null) {
            showInfoAlert("Немає обраної сесії", "Оберіть призупинену сесію зі списку для відновлення.");
            return;
        }
        if (selected.distributed()) {
            showInfoAlert(
                    "Розподілена сесія",
                    "Сесія #" + selected.id() + " є розподіленою. "
                            + "Для її відновлення вкажіть порт і натисніть «Відновити розподілену сесію»."
            );
            return;
        }

        runAsyncAction("Відновлення сесії #" + selected.id(), () -> {
            crawlService.setCacheTtlHours(cacheTtlHours);
            crawlService.resume(selected.id());
            Platform.runLater(() -> {
                CrawlSession current = crawlService.getActiveSession();
                if (current != null) {
                    updateCountersFromSession(current);
                }
                refreshSiteTree();
                refreshPausedSessionsAsync();
            });
        }, true);
    }

    @FXML
    private void onStopCrawl() {
        if (!ensureServiceAvailable()) {
            return;
        }

        CrawlSession active = crawlService.getActiveSession();
        boolean hasActiveStoppable = active != null
                && (active.getStatus() == SessionStatus.RUNNING || active.getStatus() == SessionStatus.PAUSED);
        PausedSessionItem selectedPaused = pausedSessionsComboBox.getValue();

        if (!hasActiveStoppable && selectedPaused == null) {
            showInfoAlert("Немає активної сесії", "Немає активної або призупиненої сесії для зупинки.");
            return;
        }

        if (!confirmPermanentStop()) {
            return;
        }

        if (hasActiveStoppable) {
            runAsyncAction("Зупинка обходу", () -> {
                crawlService.stop();
                Platform.runLater(this::refreshPausedSessionsAsync);
            }, true);
        } else {
            long targetId = selectedPaused.id();
            runAsyncAction("Зупинка сесії #" + targetId, () -> {
                crawlService.stopSession(targetId);
                Platform.runLater(this::refreshPausedSessionsAsync);
            }, true);
        }
    }

    @FXML
    private void onRefreshPausedSessions() {
        if (!ensureServiceAvailable()) {
            return;
        }
        refreshPausedSessionsAsync();
    }

    @FXML
    private void onClearCache() {
        if (!ensureServiceAvailable()) {
            return;
        }
        runAsyncAction("Очищення кешу", () -> crawlService.clearCache(), false);
    }

    @FXML
    private void onCreateDistributedSession() {
        if (!ensureServiceAvailable()) {
            return;
        }
        CrawlSettings settings;
        int port;
        int cacheTtlHours;
        try {
            settings = buildAndValidateSettings();
            port = parsePort(initiatorPortField.getText());
            cacheTtlHours = readSpinnerInt(cacheTtlSpinner, "Термін дії кешу (год)", 0, 8760);
        } catch (IllegalArgumentException ex) {
            showErrorAlert("Помилка параметрів розподіленої сесії", ex.getMessage());
            appendLog("[ПОМИЛКА] " + ex.getMessage());
            return;
        }

        resetCountersForNewRun("Створення розподіленої сесії...");
        runAsyncAction("Створення розподіленої сесії", () -> {
            crawlService.setCacheTtlHours(cacheTtlHours);
            crawlService.createDistributedSession(settings, port);
            Platform.runLater(() -> {
                CrawlSession session = crawlService.getActiveSession();
                if (session != null) {
                    updateCountersFromSession(session);
                }
                refreshPeersDisplay();
                refreshSiteTree();
            });
        }, true);
    }

    @FXML
    private void onResumeDistributedSession() {
        if (!ensureServiceAvailable()) {
            return;
        }
        int port;
        int cacheTtlHours;
        try {
            port = parsePort(initiatorPortField.getText());
            cacheTtlHours = readSpinnerInt(cacheTtlSpinner, "Термін дії кешу (год)", 0, 8760);
        } catch (IllegalArgumentException ex) {
            showErrorAlert("Помилка відновлення розподіленої сесії", ex.getMessage());
            return;
        }

        Long targetSessionId = resolveDistributedSessionToResume();
        if (targetSessionId == null) {
            showInfoAlert(
                    "Оберіть розподілену сесію",
                    "Оберіть призупинену розподілену сесію [P2P] зі списку призупинених сесій."
            );
            return;
        }

        runAsyncAction("Відновлення розподіленої сесії #" + targetSessionId, () -> {
            crawlService.setCacheTtlHours(cacheTtlHours);
            crawlService.resumeDistributedSession(targetSessionId, port);
            Platform.runLater(() -> {
                CrawlSession current = crawlService.getActiveSession();
                if (current != null) {
                    updateCountersFromSession(current);
                }
                refreshPeersDisplay();
                refreshSiteTree();
                refreshPausedSessionsAsync();
            });
        }, true);
    }

    @FXML
    private void onConnectPeer() {
        if (!ensureServiceAvailable()) {
            return;
        }
        String host = workerHostField.getText() != null ? workerHostField.getText().trim() : "";
        if (host.isEmpty()) {
            showErrorAlert("Помилка підключення", "Вкажіть IP-адресу або хост ініціатора.");
            return;
        }
        int port;
        int cacheTtlHours;
        try {
            port = parsePort(workerPortField.getText());
            cacheTtlHours = readSpinnerInt(cacheTtlSpinner, "Термін дії кешу (год)", 0, 8760);
        } catch (IllegalArgumentException ex) {
            showErrorAlert("Помилка підключення", ex.getMessage());
            return;
        }

        resetCountersForNewRun("Підключення до " + host + ":" + port + "...");
        runAsyncAction("Підключення до сесії P2P", () -> {
            crawlService.setCacheTtlHours(cacheTtlHours);
            crawlService.connectToSession(host, port);
            Platform.runLater(() -> {
                sessionStatusLabel.setText(crawlService.isPaused() ? "Пауза (вузол)" : "Вузол активний");
                refreshPeersDisplay();
            });
        }, true);
    }

    @FXML
    private void onDisconnectPeer() {
        if (!ensureServiceAvailable()) {
            return;
        }
        runAsyncAction("Відключення від сесії", () -> {
            crawlService.disconnectFromSession();
            Platform.runLater(() -> {
                sessionStatusLabel.setText("Відключено");
                refreshPeersDisplay();
            });
        }, false);
    }

    @FXML
    private void onClearLog() {
        eventLogListView.getItems().clear();
    }

    public void handleProgressUpdated(CrawlSession session) {
        updateCountersFromSession(session);
        refreshPeersDisplay();
        maybeRefreshSiteTree();
        updateUiState();
    }

    public void handlePageProcessed(Page page) {
        if (page == null) {
            return;
        }
        if (crawlService != null && crawlService.isConnectedPeer() && crawlService.getActiveSession() == null) {
            peerVisitedCount++;
            if (page.isHasTerm()) {
                peerMatchedCount++;
            }
            if ((page.getError() != null && !page.getError().isBlank()) || page.getHttpStatus() >= 400) {
                peerErrorCount++;
            }
            visitedCountLabel.setText(String.valueOf(peerVisitedCount));
            matchedCountLabel.setText(String.valueOf(peerMatchedCount));
            errorCountLabel.setText(String.valueOf(peerErrorCount));
            sessionStatusLabel.setText(crawlService.isPaused() ? "Пауза (вузол)" : "Вузол активний");
        }
        if (page.isHasTerm()) {
            appendLog("[ЗБІГ] " + page.getUrl() + " -> " + (page.getFilePath() != null ? page.getFilePath() : "збережено"));
        }
        maybeRefreshSiteTree();
    }

    public void handleError(String url, String errorMessage) {
        String prefix = url != null && !url.isBlank() ? url + " — " : "";
        appendLog("[ПОМИЛКА] " + prefix + (errorMessage != null ? errorMessage : "Невідома помилка"));
        updateUiState();
    }

    public void handleSessionPaused(CrawlSession session) {
        updateCountersFromSession(session);
        refreshSiteTree();
        refreshPeersDisplay();
        refreshPausedSessionsAsync();
        if (session != null) {
            appendLog("Сесію #" + session.getId() + " призупинено.");
        }
        updateUiState();
    }

    public void handleSessionFinished(CrawlSession session) {
        updateCountersFromSession(session);
        refreshSiteTree();
        refreshPeersDisplay();
        refreshPausedSessionsAsync();
        if (session != null) {
            String reason = session.getCompletionReason() != null ? " (" + session.getCompletionReason() + ")" : "";
            appendLog("Сесію #" + session.getId() + " завершено" + reason + ".");
        }
        updateUiState();
    }

    public void handlePeerConnected(PeerNode peer) {
        if (peer != null) {
            appendLog("Підключився вузол: " + peer.getId() + " (" + peer.getHost() + ":" + peer.getPort() + ")");
        }
        refreshPeersDisplay();
        updateUiState();
    }

    public void handlePeerLost(PeerNode peer) {
        if (peer != null) {
            appendLog("[ВТРАТА ВУЗЛА] " + peer.getId() + " (" + peer.getHost() + ":" + peer.getPort() + ")");
        }
        refreshPeersDisplay();
        updateUiState();
    }

    public void handleStatusMessage(String message) {
        if (message != null && !message.isBlank()) {
            appendLog(message);
            if (crawlService != null && !crawlService.isConnectedPeer() && crawlService.getActiveSession() == null) {
                String lower = message.toLowerCase();
                if (lower.contains("втрачено зв'язок")) {
                    sessionStatusLabel.setText("Зв'язок втрачено");
                } else if (lower.contains("завершено ініціатором") || lower.contains("відключився")) {
                    sessionStatusLabel.setText("Відключено");
                }
            }
        }
        refreshPeersDisplay();
        updateUiState();
    }

    public Button getStartButton() {
        return startButton;
    }

    public Button getPauseButton() {
        return pauseButton;
    }

    public Button getResumeButton() {
        return resumeButton;
    }

    public Button getStopButton() {
        return stopButton;
    }

    public Button getClearCacheButton() {
        return clearCacheButton;
    }

    public Button getConnectPeerButton() {
        return connectPeerButton;
    }

    public Button getCreateDistributedButton() {
        return createDistributedButton;
    }

    public Map<FilterType, CheckBox> getFilterCheckBoxes() {
        return Map.copyOf(filterCheckBoxes);
    }

    private CrawlSettings buildAndValidateSettings() {
        int maxDepth = readSpinnerInt(maxDepthSpinner, "Максимальна глибина", 0, 100);
        int pageLimit = readSpinnerInt(pageLimitSpinner, "Ліміт сторінок", 1, 100_000);
        StrategyOption selectedStrategy = strategyComboBox.getValue();
        StrategyType strategy = selectedStrategy != null ? selectedStrategy.type() : StrategyType.BFS;

        EnumSet<FilterType> enabledFilters = EnumSet.noneOf(FilterType.class);
        for (Map.Entry<FilterType, CheckBox> entry : filterCheckBoxes.entrySet()) {
            if (entry.getValue().isSelected()) {
                enabledFilters.add(entry.getKey());
            }
        }

        CrawlSettings settings = new CrawlSettings(
                startUrlField.getText() != null ? startUrlField.getText().trim() : "",
                searchTermField.getText() != null ? searchTermField.getText().trim() : "",
                strategy,
                maxDepth,
                pageLimit,
                wholeWordCheckBox.isSelected(),
                includeSubdomainsCheckBox.isSelected(),
                enabledFilters
        );

        Optional<String> validationError = settings.validateWithMessage();
        if (validationError.isPresent()) {
            throw new IllegalArgumentException(validationError.get());
        }
        return settings;
    }

    private Long resolveDistributedSessionToResume() {
        PausedSessionItem selected = pausedSessionsComboBox.getValue();
        if (selected != null) {
            if (!selected.distributed()) {
                throw new IllegalArgumentException("Обрана сесія #" + selected.id() + " є локальною, а не розподіленою.");
            }
            return selected.id();
        }
        CrawlSession active = crawlService.getActiveSession();
        if (active != null && active.getStatus() == SessionStatus.PAUSED && active.isDistributed()) {
            return active.getId();
        }
        return null;
    }

    private boolean confirmPermanentStop() {
        ButtonType stopConfirmType = new ButtonType("Зупинити", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancelType = new ButtonType("Скасувати", ButtonBar.ButtonData.CANCEL_CLOSE);
        Alert confirmAlert = new Alert(Alert.AlertType.CONFIRMATION, "", stopConfirmType, cancelType);
        confirmAlert.setTitle("Підтвердження зупинки");
        confirmAlert.setHeaderText("Остаточно зупинити обхід?");
        confirmAlert.setContentText(
                "Після зупинки поточну сесію неможливо буде відновити. "
                        + "Вже збережені HTML-файли та метадані залишаться в базі даних і на диску."
        );
        Optional<ButtonType> choice = confirmAlert.showAndWait();
        return choice.isPresent() && choice.get() == stopConfirmType;
    }

    private void refreshPausedSessionsAsync() {
        if (crawlService == null) {
            return;
        }
        actionExecutor.submit(() -> {
            try {
                List<Long> pausedIds = crawlService.getPausedSessions();
                List<PausedSessionItem> items = new ArrayList<>();
                for (Long id : pausedIds) {
                    Optional<CrawlSession> sessionOpt = crawlService.findSessionById(id);
                    if (sessionOpt.isPresent()) {
                        CrawlSession s = sessionOpt.get();
                        items.add(new PausedSessionItem(
                                s.getId(),
                                s.getSettings().getStartUrl(),
                                s.getSettings().getSearchTerm(),
                                s.getVisitedCount(),
                                s.isDistributed(),
                                s.getServerPort()
                        ));
                    }
                }
                Platform.runLater(() -> applyPausedSessionsList(items));
            } catch (RuntimeException ex) {
                LOGGER.log(Level.WARNING, "Failed to refresh paused sessions list", ex);
            }
        });
    }

    private void applyPausedSessionsList(List<PausedSessionItem> items) {
        PausedSessionItem previous = pausedSessionsComboBox.getValue();
        Long previousId = previous != null ? previous.id() : null;

        updatingPausedList = true;
        try {
            pausedSessionsComboBox.getItems().setAll(items);
            if (previousId != null) {
                for (PausedSessionItem item : items) {
                    if (item.id() == previousId) {
                        pausedSessionsComboBox.setValue(item);
                        break;
                    }
                }
            } else if (!items.isEmpty()) {
                pausedSessionsComboBox.getSelectionModel().selectFirst();
            }
        } finally {
            updatingPausedList = false;
        }
        updateUiState();
    }

    private void runAsyncAction(String actionName, ThrowingRunnable work, boolean showErrorsAsAlert) {
        if (actionInProgress) {
            return;
        }
        actionInProgress = true;
        updateUiState();

        actionExecutor.submit(() -> {
            try {
                work.run();
            } catch (Exception ex) {
                LOGGER.log(Level.WARNING, actionName + " failed", ex);
                String message = ex.getMessage() != null && !ex.getMessage().isBlank()
                        ? ex.getMessage()
                        : "Не вдалося виконати дію: " + actionName + ".";
                Platform.runLater(() -> {
                    appendLog("[ПОМИЛКА] " + message);
                    if (showErrorsAsAlert) {
                        showErrorAlert(actionName, message);
                    }
                });
            } finally {
                Platform.runLater(() -> {
                    actionInProgress = false;
                    refreshPeersDisplay();
                    updateUiState();
                });
            }
        });
    }

    private void updateUiState() {
        if (crawlService == null) {
            applyButtonStates(false, true, false, false, false, true, false, true, false, true);
            return;
        }
        if (actionInProgress) {
            applyButtonStates(true, false, false, false, false, false, false, false, false, false);
            return;
        }
        if (crawlService.isConnectedPeer()) {
            applyButtonStates(true, false, false, false, false, false, false, false, true, false);
            return;
        }

        CrawlSession active = crawlService.getActiveSession();
        SessionStatus activeStatus = active != null ? active.getStatus() : SessionStatus.IDLE;
        boolean running = crawlService.isRunning();
        boolean distributedServerOpen = crawlService.isDistributedInitiator();

        if (running || (distributedServerOpen && activeStatus == SessionStatus.RUNNING)) {
            applyButtonStates(true, false, true, false, true, false, false, false, false, false);
            return;
        }

        if (active != null && activeStatus == SessionStatus.PAUSED) {
            boolean isDistributed = active.isDistributed();
            boolean hasOpenServer = crawlService.getSessionCoordinator() != null;
            applyButtonStates(
                    true,
                    false,
                    false,
                    !isDistributed || hasOpenServer,
                    true,
                    false,
                    isDistributed && !hasOpenServer,
                    false,
                    false,
                    true
            );
            return;
        }

        PausedSessionItem selectedPaused = pausedSessionsComboBox.getValue();
        boolean hasSelectedLocalPaused = selectedPaused != null && !selectedPaused.distributed();
        boolean hasSelectedDistPaused = selectedPaused != null && selectedPaused.distributed();
        applyButtonStates(
                false,
                true,
                false,
                hasSelectedLocalPaused,
                selectedPaused != null,
                true,
                hasSelectedDistPaused,
                true,
                false,
                true
        );
    }

    private void applyButtonStates(
            boolean formDisabled,
            boolean startEnabled,
            boolean pauseEnabled,
            boolean resumeEnabled,
            boolean stopEnabled,
            boolean createDistEnabled,
            boolean resumeDistEnabled,
            boolean connectEnabled,
            boolean disconnectEnabled,
            boolean clearCacheEnabled) {
        setFormControlsDisabled(formDisabled);
        startButton.setDisable(!startEnabled);
        pauseButton.setDisable(!pauseEnabled);
        resumeButton.setDisable(!resumeEnabled);
        stopButton.setDisable(!stopEnabled);
        createDistributedButton.setDisable(!createDistEnabled);
        resumeDistributedButton.setDisable(!resumeDistEnabled);
        connectPeerButton.setDisable(!connectEnabled);
        disconnectPeerButton.setDisable(!disconnectEnabled);
        clearCacheButton.setDisable(!clearCacheEnabled);
    }

    private void setFormControlsDisabled(boolean disabled) {
        startUrlField.setDisable(disabled);
        searchTermField.setDisable(disabled);
        strategyComboBox.setDisable(disabled);
        maxDepthSpinner.setDisable(disabled);
        pageLimitSpinner.setDisable(disabled);
        wholeWordCheckBox.setDisable(disabled);
        includeSubdomainsCheckBox.setDisable(disabled);
        for (CheckBox cb : filterCheckBoxes.values()) {
            cb.setDisable(disabled);
        }
        pausedSessionsComboBox.setDisable(disabled);
        refreshPausedButton.setDisable(disabled);
        cacheTtlSpinner.setDisable(disabled);
        initiatorPortField.setDisable(disabled);
        workerHostField.setDisable(disabled);
        workerPortField.setDisable(disabled);
    }

    private void updateCountersFromSession(CrawlSession session) {
        if (session == null) {
            return;
        }
        visitedCountLabel.setText(String.valueOf(session.getVisitedCount()));
        matchedCountLabel.setText(String.valueOf(session.getMatchedCount()));
        errorCountLabel.setText(String.valueOf(session.getErrorCount()));
        sessionStatusLabel.setText(formatSessionStatus(session));
    }

    private void resetCountersForNewRun(String statusText) {
        peerVisitedCount = 0;
        peerMatchedCount = 0;
        peerErrorCount = 0;
        visitedCountLabel.setText("0");
        matchedCountLabel.setText("0");
        errorCountLabel.setText("0");
        sessionStatusLabel.setText(statusText);
        resetSiteTree();
    }

    private void refreshPeersDisplay() {
        if (crawlService == null) {
            peersTable.getItems().clear();
            workersCountLabel.setText("Активних виконавців: 1");
            return;
        }
        List<PeerNode> peers = crawlService.getConnectedPeers();
        peersTable.getItems().setAll(peers);
        int activeWorkers = crawlService.getActiveWorkersCount();
        workersCountLabel.setText("Активних виконавців: " + activeWorkers
                + (peers.isEmpty() ? "" : " (підключено вузлів: " + peers.size() + ")"));
    }

    private void maybeRefreshSiteTree() {
        long now = System.nanoTime();
        if (now - lastTreeRefreshNanos >= TREE_REFRESH_THROTTLE_NANOS) {
            lastTreeRefreshNanos = now;
            refreshSiteTree();
        }
    }

    private void refreshSiteTree() {
        if (crawlService == null) {
            resetSiteTree();
            return;
        }
        Map<String, Site> snapshot = crawlService.getSitesSnapshot();
        if (snapshot.isEmpty()) {
            resetSiteTree();
            return;
        }

        TreeItem<String> root = new TreeItem<>("Результати обходу");
        root.setExpanded(true);

        List<Site> sortedSites = snapshot.values().stream()
                .sorted(Comparator.comparing(Site::getHost))
                .toList();

        AtomicInteger savedFilesCount = new AtomicInteger(0);
        for (Site site : sortedSites) {
            TreeItem<String> siteNode = new TreeItem<>(
                    "🌐 " + site.getHost() + " (відвідано: " + site.getPages().size() + ")"
            );
            siteNode.setExpanded(true);

            DirectoryElement rootDirectory = site.getRootDirectory();
            if (rootDirectory != null) {
                for (SiteElement child : rootDirectory.getChildren()) {
                    siteNode.getChildren().add(buildSiteTreeItem(child, savedFilesCount));
                }
            }
            root.getChildren().add(siteNode);
        }

        siteTreeView.setRoot(root);
        treeSummaryLabel.setText(sortedSites.size() + " сайтів, " + savedFilesCount.get() + " збережених файлів");
    }

    private TreeItem<String> buildSiteTreeItem(SiteElement element, AtomicInteger savedFilesCount) {
        if (element instanceof DirectoryElement directory) {
            TreeItem<String> dirNode = new TreeItem<>("📁 " + directory.getName());
            dirNode.setExpanded(true);
            for (SiteElement child : directory.getChildren()) {
                dirNode.getChildren().add(buildSiteTreeItem(child, savedFilesCount));
            }
            return dirNode;
        }
        if (element instanceof PageElement pageElement) {
            savedFilesCount.incrementAndGet();
            Page page = pageElement.getPage();
            String titlePart = (page != null && page.getTitle() != null && !page.getTitle().isBlank())
                    ? " — " + page.getTitle()
                    : "";
            return new TreeItem<>("📄 " + pageElement.getName() + titlePart);
        }
        return new TreeItem<>(element.getName());
    }

    private void resetSiteTree() {
        TreeItem<String> emptyRoot = new TreeItem<>("Результати обходу (збережені файли відсутні)");
        emptyRoot.setExpanded(true);
        siteTreeView.setRoot(emptyRoot);
        treeSummaryLabel.setText("0 сайтів, 0 файлів");
    }

    private void configurePeersTable() {
        peersTable.setPlaceholder(new Label("Немає підключених вузлів-учасників"));
        peerIdColumn.setCellValueFactory(cell -> new SimpleStringProperty(cell.getValue().getId()));
        peerHostColumn.setCellValueFactory(cell -> new SimpleStringProperty(cell.getValue().getHost()));
        peerPortColumn.setCellValueFactory(cell -> new SimpleIntegerProperty(cell.getValue().getPort()));
        peerHeartbeatColumn.setCellValueFactory(cell -> {
            LocalDateTime hb = cell.getValue().getLastHeartbeat();
            String formatted = hb != null ? LOG_TIME_FORMATTER.format(hb) : "—";
            return new SimpleStringProperty(formatted);
        });
    }

    private void appendLog(String message) {
        if (message == null || message.isBlank()) {
            return;
        }
        String timestamped = "[" + LOG_TIME_FORMATTER.format(LocalTime.now()) + "] " + message;
        var items = eventLogListView.getItems();
        items.add(timestamped);
        if (items.size() > MAX_LOG_ENTRIES) {
            items.remove(0, items.size() - MAX_LOG_ENTRIES);
        }
        eventLogListView.scrollTo(items.size() - 1);
    }

    private boolean ensureServiceAvailable() {
        if (crawlService == null) {
            showErrorAlert("Сервіс недоступний", "Сервіс обходу ще не ініціалізовано.");
            return false;
        }
        return true;
    }

    private static void configureIntSpinner(Spinner<Integer> spinner, int min, int max, int initialValue) {
        SpinnerValueFactory.IntegerSpinnerValueFactory factory =
                new SpinnerValueFactory.IntegerSpinnerValueFactory(min, max, initialValue);
        spinner.setValueFactory(factory);
        spinner.setEditable(true);
        spinner.focusedProperty().addListener((obs, wasFocused, isFocused) -> {
            if (!isFocused) {
                commitSpinnerQuietly(spinner, min, max);
            }
        });
    }

    private static void commitSpinnerQuietly(Spinner<Integer> spinner, int min, int max) {
        String text = spinner.getEditor().getText();
        if (text == null || text.isBlank()) {
            spinner.getEditor().setText(String.valueOf(spinner.getValue()));
            return;
        }
        try {
            int parsed = Integer.parseInt(text.trim());
            int clamped = Math.max(min, Math.min(max, parsed));
            spinner.getValueFactory().setValue(clamped);
            spinner.getEditor().setText(String.valueOf(clamped));
        } catch (NumberFormatException ex) {
            spinner.getEditor().setText(String.valueOf(spinner.getValue()));
        }
    }

    private static int readSpinnerInt(Spinner<Integer> spinner, String fieldName, int min, int max) {
        String text = spinner.getEditor().getText();
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Поле «" + fieldName + "» не може бути порожнім.");
        }
        int parsed;
        try {
            parsed = Integer.parseInt(text.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Поле «" + fieldName + "» має містити ціле число.");
        }
        if (parsed < min || parsed > max) {
            throw new IllegalArgumentException(
                    "Поле «" + fieldName + "» має бути в межах від " + min + " до " + max + "."
            );
        }
        spinner.getValueFactory().setValue(parsed);
        return parsed;
    }

    static int parsePort(String rawPort) {
        if (rawPort == null || rawPort.isBlank()) {
            throw new IllegalArgumentException("Вкажіть номер TCP-порту.");
        }
        int port;
        try {
            port = Integer.parseInt(rawPort.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Номер порту має бути цілим числом.");
        }
        if (port < MIN_P2P_PORT || port > MAX_P2P_PORT) {
            throw new IllegalArgumentException("Порт має бути в діапазоні 1024..65535.");
        }
        return port;
    }

    private static String formatSessionStatus(CrawlSession session) {
        String idSuffix = session.getId() > 0 ? " (#" + session.getId() + ")" : "";
        return switch (session.getStatus()) {
            case IDLE -> "Очікування";
            case RUNNING -> "Виконується" + idSuffix;
            case PAUSED -> "Призупинено" + idSuffix;
            case FINISHED -> "Завершено" + idSuffix;
            case FAILED -> "Помилка" + idSuffix;
        };
    }

    private static void showErrorAlert(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR, message, ButtonType.OK);
        alert.setTitle(title);
        alert.setHeaderText(title);
        alert.showAndWait();
    }

    private static void showInfoAlert(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION, message, ButtonType.OK);
        alert.setTitle(title);
        alert.setHeaderText(title);
        alert.showAndWait();
    }

    private static void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
        } else {
            Platform.runLater(action);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private record StrategyOption(StrategyType type, String label) {
        @Override
        public String toString() {
            return label;
        }
    }

    private record PausedSessionItem(
            long id,
            String startUrl,
            String searchTerm,
            int visitedCount,
            boolean distributed,
            Integer serverPort
    ) {
        @Override
        public String toString() {
            String mode = distributed
                    ? " [P2P" + (serverPort != null ? ":" + serverPort : "") + "]"
                    : " [Локальна]";
            return "#" + id + " — " + startUrl + " | «" + searchTerm + "» (відвідано: " + visitedCount + ")" + mode;
        }
    }
}
