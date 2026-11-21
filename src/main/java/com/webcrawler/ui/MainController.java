package com.webcrawler.ui;

import com.webcrawler.core.CrawlEventListener;
import com.webcrawler.core.CrawlService;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.Page;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.p2p.PeerNode;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;

import java.util.Objects;

public class MainController {

    private static final long STATS_LIVE_REFRESH_INTERVAL_NANOS = 1_000_000_000L;

    @FXML
    private TabPane mainTabPane;
    @FXML
    private Tab crawlTabItem;
    @FXML
    private Tab statisticsTabItem;
    @FXML
    private CrawlTabController crawlTabController;
    @FXML
    private StatisticsTabController statisticsTabController;
    @FXML
    private Label headerModeLabel;
    @FXML
    private Label headerStatusBadge;
    @FXML
    private Label footerStatusLabel;
    @FXML
    private Label footerConnectionLabel;
    @FXML
    private Label footerDataDirLabel;

    private CrawlService crawlService;
    private CrawlEventListener serviceListener;
    private long lastStatsRefreshNanos;

    @FXML
    private void initialize() {
        if (mainTabPane != null) {
            mainTabPane.getSelectionModel().selectedItemProperty().addListener((obs, oldTab, newTab) -> {
                if (newTab == statisticsTabItem && statisticsTabController != null && crawlService != null) {
                    statisticsTabController.refresh();
                }
            });
        }
    }

    public void setCrawlService(CrawlService crawlService) {
        Objects.requireNonNull(crawlService, "crawlService must not be null");
        if (this.crawlService != null && this.serviceListener != null) {
            this.crawlService.removeEventListener(this.serviceListener);
        }

        this.crawlService = crawlService;
        if (crawlTabController != null) {
            crawlTabController.setCrawlService(crawlService);
        }
        if (statisticsTabController != null) {
            statisticsTabController.setCrawlService(crawlService);
        }

        this.serviceListener = createFxEventListener();
        crawlService.addEventListener(this.serviceListener);

        runOnFxThread(() -> {
            if (footerDataDirLabel != null) {
                footerDataDirLabel.setText("Каталог даних: " + crawlService.getAppPaths().getBaseDir());
            }
            updateHeaderAndConnectionIndicators();
        });
    }

    public void showClosingState() {
        runOnFxThread(() -> {
            if (footerStatusLabel != null) {
                footerStatusLabel.setText("Завершення роботи та збереження стану...");
            }
            applyStatusBadge("Завершення...", "status-paused");
        });
    }

    public void shutdown() {
        if (crawlService != null && serviceListener != null) {
            crawlService.removeEventListener(serviceListener);
        }
        if (crawlTabController != null) {
            crawlTabController.shutdown();
        }
        if (statisticsTabController != null) {
            statisticsTabController.shutdown();
        }
    }

    public TabPane getMainTabPane() {
        return mainTabPane;
    }

    public CrawlTabController getCrawlTabController() {
        return crawlTabController;
    }

    public StatisticsTabController getStatisticsTabController() {
        return statisticsTabController;
    }

    private CrawlEventListener createFxEventListener() {
        return new CrawlEventListener() {
            @Override
            public void onProgressUpdated(CrawlSession session) {
                runOnFxThread(() -> {
                    if (crawlTabController != null) {
                        crawlTabController.handleProgressUpdated(session);
                    }
                    updateHeaderAndConnectionIndicators();
                    maybeRefreshStatisticsDuringCrawl();
                });
            }

            @Override
            public void onPageProcessed(Page page) {
                runOnFxThread(() -> {
                    if (crawlTabController != null) {
                        crawlTabController.handlePageProcessed(page);
                    }
                });
            }

            @Override
            public void onError(String url, String errorMessage) {
                runOnFxThread(() -> {
                    if (crawlTabController != null) {
                        crawlTabController.handleError(url, errorMessage);
                    }
                    if (footerStatusLabel != null && errorMessage != null && !errorMessage.isBlank()) {
                        footerStatusLabel.setText("Помилка (" + (url != null ? url : "—") + "): " + errorMessage);
                    }
                    updateHeaderAndConnectionIndicators();
                });
            }

            @Override
            public void onSessionPaused(CrawlSession session) {
                runOnFxThread(() -> {
                    if (crawlTabController != null) {
                        crawlTabController.handleSessionPaused(session);
                    }
                    if (statisticsTabController != null) {
                        statisticsTabController.refresh();
                    }
                    updateHeaderAndConnectionIndicators();
                });
            }

            @Override
            public void onSessionFinished(CrawlSession session) {
                runOnFxThread(() -> {
                    if (crawlTabController != null) {
                        crawlTabController.handleSessionFinished(session);
                    }
                    if (statisticsTabController != null) {
                        statisticsTabController.refresh();
                    }
                    updateHeaderAndConnectionIndicators();
                });
            }

            @Override
            public void onPeerConnected(PeerNode peer) {
                runOnFxThread(() -> {
                    if (crawlTabController != null) {
                        crawlTabController.handlePeerConnected(peer);
                    }
                    updateHeaderAndConnectionIndicators();
                });
            }

            @Override
            public void onPeerLost(PeerNode peer) {
                runOnFxThread(() -> {
                    if (crawlTabController != null) {
                        crawlTabController.handlePeerLost(peer);
                    }
                    updateHeaderAndConnectionIndicators();
                });
            }

            @Override
            public void onStatusMessage(String message) {
                runOnFxThread(() -> {
                    if (footerStatusLabel != null && message != null && !message.isBlank()) {
                        footerStatusLabel.setText(message);
                    }
                    if (crawlTabController != null) {
                        crawlTabController.handleStatusMessage(message);
                    }
                    updateHeaderAndConnectionIndicators();
                });
            }
        };
    }

    private void maybeRefreshStatisticsDuringCrawl() {
        if (statisticsTabController == null || mainTabPane == null) {
            return;
        }
        if (mainTabPane.getSelectionModel().getSelectedItem() != statisticsTabItem) {
            return;
        }
        long now = System.nanoTime();
        if (now - lastStatsRefreshNanos >= STATS_LIVE_REFRESH_INTERVAL_NANOS) {
            lastStatsRefreshNanos = now;
            statisticsTabController.refresh();
        }
    }

    private void updateHeaderAndConnectionIndicators() {
        if (crawlService == null) {
            return;
        }
        if (crawlService.isConnectedPeer()) {
            int workers = crawlService.getActiveWorkersCount();
            if (headerModeLabel != null) {
                headerModeLabel.setText("Режим: Вузол-учасник P2P");
            }
            if (footerConnectionLabel != null) {
                footerConnectionLabel.setText("Підключення: вузол-учасник (виконавців: " + workers + ")");
            }
            if (crawlService.isPaused()) {
                applyStatusBadge("Пауза (вузол)", "status-paused");
            } else {
                applyStatusBadge("Вузол активний", "status-peer");
            }
            return;
        }

        if (crawlService.isDistributedInitiator()) {
            int port = crawlService.getServerPort();
            int peers = crawlService.getConnectedPeerCount();
            if (headerModeLabel != null) {
                headerModeLabel.setText("Режим: Ініціатор P2P (порт " + port + ")");
            }
            if (footerConnectionLabel != null) {
                footerConnectionLabel.setText("Підключення: ініціатор (порт " + port + ", вузлів: " + peers + ")");
            }
            CrawlSession session = crawlService.getActiveSession();
            applySessionStatusBadge(session != null ? session.getStatus() : SessionStatus.RUNNING);
            return;
        }

        if (headerModeLabel != null) {
            headerModeLabel.setText("Режим: Локальний");
        }
        if (footerConnectionLabel != null) {
            footerConnectionLabel.setText("Підключення: локальний режим");
        }
        CrawlSession session = crawlService.getActiveSession();
        applySessionStatusBadge(session != null ? session.getStatus() : SessionStatus.IDLE);
    }

    private void applySessionStatusBadge(SessionStatus status) {
        SessionStatus effective = status != null ? status : SessionStatus.IDLE;
        switch (effective) {
            case IDLE -> applyStatusBadge("Очікування", "status-idle");
            case RUNNING -> applyStatusBadge("Виконується", "status-running");
            case PAUSED -> applyStatusBadge("Призупинено", "status-paused");
            case FINISHED -> applyStatusBadge("Завершено", "status-finished");
            case FAILED -> applyStatusBadge("Помилка", "status-failed");
        }
    }

    private void applyStatusBadge(String text, String statusClass) {
        if (headerStatusBadge == null) {
            return;
        }
        headerStatusBadge.setText(text);
        headerStatusBadge.getStyleClass().removeAll(
                "status-idle", "status-running", "status-paused", "status-finished", "status-failed", "status-peer"
        );
        headerStatusBadge.getStyleClass().add(statusClass);
    }

    private static void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
        } else {
            Platform.runLater(action);
        }
    }
}
