package com.webcrawler.ui;

import com.webcrawler.config.AppPaths;
import com.webcrawler.core.CrawlService;
import com.webcrawler.model.CrawlSession;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableView;
import javafx.scene.control.TitledPane;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UiFxmlLoadTest {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void initJavaFxToolkit() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        try {
            Platform.startup(() -> {
                Platform.setImplicitExit(false);
                started.countDown();
            });
            assertTrue(started.await(15, TimeUnit.SECONDS), "JavaFX toolkit failed to start");
        } catch (IllegalStateException alreadyInitialized) {
            Platform.setImplicitExit(false);
        }
    }

    @Test
    void statisticsTabFxmlShowsEmptyStateThenDisplaysPersistedSessionMetrics() throws Exception {
        CrawlService service = new CrawlService(new AppPaths(tempDir.resolve("stats-tab")));
        try {
            LoadedView<StatisticsTabController> loaded = loadOnFxThread("/fxml/statistics-tab.fxml");
            StatisticsTabController controller = loaded.controller();
            assertNotNull(controller);

            runOnFxThreadAndWait(() -> controller.setCrawlService(service));
            awaitFxCondition(() -> controller.getEmptyStateBox().isVisible());

            assertTrue(controller.getEmptyStateBox().isVisible());
            assertFalse(controller.getContentContainer().isVisible());
            assertEquals(
                    "Усі сесії (загальна статистика)",
                    controller.getSessionSelectorComboBox().getValue().toString()
            );

            CrawlSettings settings = new CrawlSettings("https://stats.example/", "тест");
            CrawlSession session = new CrawlSession(settings);
            session.start();
            session.finish("Завершено");
            session.setVisitedCount(1);
            session.setMatchedCount(1);
            service.getSessionRepository().create(session);

            Page page = new Page(
                    0L,
                    session.getId(),
                    0L,
                    "https://stats.example/",
                    "Тестова сторінка",
                    LocalDateTime.now(),
                    1536L,
                    0,
                    200,
                    null,
                    true,
                    "results/stats.example/index.html"
            );
            service.getPageRepository().save(page, "stats.example");

            TableView<?> hostTable = findNodeById(loaded.root(), "hostTable", TableView.class);
            TableView<?> pagesTable = findNodeById(loaded.root(), "pagesTable", TableView.class);
            Label visitedLabel = findNodeById(loaded.root(), "visitedKpiLabel", Label.class);
            Label matchedLabel = findNodeById(loaded.root(), "matchedKpiLabel", Label.class);

            runOnFxThreadAndWait(controller::refresh);
            awaitFxCondition(() -> controller.getContentContainer().isVisible() && !pagesTable.getItems().isEmpty());

            assertFalse(controller.getEmptyStateBox().isVisible());
            assertTrue(controller.getContentContainer().isVisible());
            assertEquals("1", visitedLabel.getText());
            assertEquals("1", matchedLabel.getText());
            assertEquals(1, hostTable.getItems().size());
            assertEquals(1, pagesTable.getItems().size());

            controller.shutdown();
        } finally {
            service.shutdown();
        }
    }

    private <T> LoadedView<T> loadOnFxThread(String fxmlResourcePath) throws Exception {
        CompletableFuture<LoadedView<T>> future = new CompletableFuture<>();
        Platform.runLater(() -> {
            try {
                URL url = Objects.requireNonNull(
                        getClass().getResource(fxmlResourcePath),
                        "Missing FXML resource: " + fxmlResourcePath
                );
                FXMLLoader loader = new FXMLLoader(url);
                Parent root = loader.load();
                future.complete(new LoadedView<>(root, loader.getController()));
            } catch (Throwable ex) {
                future.completeExceptionally(ex);
            }
        });
        return future.get(15, TimeUnit.SECONDS);
    }

    private static void runOnFxThreadAndWait(Runnable action) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Platform.runLater(() -> {
            try {
                action.run();
                future.complete(null);
            } catch (Throwable ex) {
                future.completeExceptionally(ex);
            }
        });
        future.get(15, TimeUnit.SECONDS);
    }

    private static void awaitFxCondition(BooleanSupplier condition) throws Exception {
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            CompletableFuture<Boolean> check = new CompletableFuture<>();
            Platform.runLater(() -> {
                try {
                    check.complete(condition.getAsBoolean());
                } catch (Throwable ex) {
                    check.completeExceptionally(ex);
                }
            });
            if (Boolean.TRUE.equals(check.get(5, TimeUnit.SECONDS))) {
                return;
            }
            Thread.sleep(25L);
        }
        throw new AssertionError("Timed out waiting for JavaFX condition");
    }

    private static <T extends Node> T findNodeById(Node root, String fxId, Class<T> type) {
        for (Node node : collectAllNodes(root)) {
            if (fxId.equals(node.getId()) && type.isInstance(node)) {
                return type.cast(node);
            }
        }
        throw new AssertionError("Node with id #" + fxId + " of type " + type.getSimpleName() + " not found");
    }

    private static List<Node> collectAllNodes(Node root) {
        List<Node> result = new ArrayList<>();
        Map<Node, Boolean> visited = new IdentityHashMap<>();
        collectNodesRecursive(root, result, visited);
        return result;
    }

    private static void collectNodesRecursive(Node node, List<Node> out, Map<Node, Boolean> visited) {
        if (node == null || visited.put(node, Boolean.TRUE) != null) {
            return;
        }
        out.add(node);
        if (node instanceof SplitPane splitPane) {
            for (Node item : splitPane.getItems()) {
                collectNodesRecursive(item, out, visited);
            }
        }
        if (node instanceof ScrollPane scrollPane) {
            collectNodesRecursive(scrollPane.getContent(), out, visited);
        }
        if (node instanceof TabPane tabPane) {
            for (Tab tab : tabPane.getTabs()) {
                collectNodesRecursive(tab.getContent(), out, visited);
            }
        }
        if (node instanceof TitledPane titledPane) {
            collectNodesRecursive(titledPane.getContent(), out, visited);
        }
        if (node instanceof Parent parent) {
            for (Node child : parent.getChildrenUnmodifiable()) {
                collectNodesRecursive(child, out, visited);
            }
        }
    }

    private record LoadedView<T>(Parent root, T controller) {
    }
}
