package com.webcrawler.ui;

import com.webcrawler.config.AppPaths;
import com.webcrawler.core.CrawlService;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;

import java.io.IOException;
import java.net.URL;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebCrawlerApp extends Application {

    private static final Logger LOGGER = Logger.getLogger(WebCrawlerApp.class.getName());
    private static final String APP_TITLE = "Розподілений пошуковий веб-сканер";
    private static final double DEFAULT_WIDTH = 1220;
    private static final double DEFAULT_HEIGHT = 820;
    private static final double MIN_WIDTH = 1100;
    private static final double MIN_HEIGHT = 760;

    private final AtomicBoolean closingInProgress = new AtomicBoolean(false);
    private CrawlService crawlService;
    private MainController mainController;

    public static void launchApp(String[] args) {
        launch(WebCrawlerApp.class, args != null ? args : new String[0]);
    }

    @Override
    public void start(Stage primaryStage) {
        AppPaths appPaths;
        try {
            String[] rawArgs = getParameters().getRaw().toArray(new String[0]);
            appPaths = AppPaths.fromArgs(rawArgs);
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, "Invalid --data-dir argument", ex);
            showStartupError(
                    "Помилка параметрів запуску",
                    "Некоректно задано каталог даних (--data-dir).",
                    ex.getMessage()
            );
            Platform.exit();
            return;
        }

        try {
            this.crawlService = new CrawlService(appPaths);
        } catch (RuntimeException ex) {
            LOGGER.log(Level.SEVERE, "Failed to initialize CrawlService", ex);
            showStartupError(
                    "Помилка ініціалізації",
                    "Не вдалося відкрити базу даних або робочі каталоги.",
                    ex.getMessage()
            );
            Platform.exit();
            return;
        }

        Parent root;
        try {
            URL fxmlUrl = Objects.requireNonNull(
                    WebCrawlerApp.class.getResource("/fxml/main.fxml"),
                    "Missing /fxml/main.fxml resource"
            );
            FXMLLoader loader = new FXMLLoader(fxmlUrl);
            root = loader.load();
            this.mainController = loader.getController();
            this.mainController.setCrawlService(this.crawlService);
        } catch (IOException | RuntimeException ex) {
            LOGGER.log(Level.SEVERE, "Failed to load main JavaFX interface", ex);
            shutdownSilently();
            showStartupError(
                    "Помилка завантаження інтерфейсу",
                    "Не вдалося завантажити головне вікно застосунку.",
                    ex.getMessage()
            );
            Platform.exit();
            return;
        }

        Scene scene = new Scene(root, DEFAULT_WIDTH, DEFAULT_HEIGHT);
        URL cssUrl = Objects.requireNonNull(
                WebCrawlerApp.class.getResource("/css/styles.css"),
                "Missing /css/styles.css resource"
        );
        scene.getStylesheets().add(cssUrl.toExternalForm());

        primaryStage.setTitle(APP_TITLE);
        primaryStage.setMinWidth(MIN_WIDTH);
        primaryStage.setMinHeight(MIN_HEIGHT);
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(event -> handleWindowClose(event, primaryStage));
        primaryStage.show();
    }

    @Override
    public void stop() {
        shutdownSilently();
    }

    private void handleWindowClose(WindowEvent event, Stage primaryStage) {
        event.consume();
        if (!closingInProgress.compareAndSet(false, true)) {
            return;
        }

        if (mainController != null) {
            mainController.showClosingState();
        }
        if (primaryStage.getScene() != null && primaryStage.getScene().getRoot() != null) {
            primaryStage.getScene().getRoot().setDisable(true);
        }

        Thread shutdownThread = new Thread(() -> {
            try {
                if (crawlService != null) {
                    crawlService.pauseOnExit();
                    crawlService.shutdown();
                }
                if (mainController != null) {
                    mainController.shutdown();
                }
            } catch (RuntimeException ex) {
                LOGGER.log(Level.WARNING, "Graceful shutdown on window close encountered an error", ex);
            } finally {
                Platform.runLater(() -> {
                    primaryStage.close();
                    Platform.exit();
                });
            }
        }, "webcrawler-exit-shutdown");
        shutdownThread.start();
    }

    private void shutdownSilently() {
        if (mainController != null) {
            try {
                mainController.shutdown();
            } catch (RuntimeException ex) {
                LOGGER.log(Level.FINE, "Error shutting down MainController", ex);
            }
        }
        if (crawlService != null) {
            try {
                crawlService.shutdown();
            } catch (RuntimeException ex) {
                LOGGER.log(Level.WARNING, "Error shutting down CrawlService", ex);
            }
        }
    }

    private static void showStartupError(String title, String header, String details) {
        Alert alert = new Alert(Alert.AlertType.ERROR, details != null ? details : "", ButtonType.OK);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.showAndWait();
    }
}
