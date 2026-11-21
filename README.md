# Web Crawler

A desktop application for selective web crawling. It follows links from a start URL, keeps only pages that contain a given term, strips non-semantic noise (scripts, styles, ads, navigation), saves the result as a tree of clean HTML files and records crawl statistics and page metadata. Several instances can share one crawl over TCP (hybrid P2P).

Built as a university coursework project. The user interface is in Ukrainian.

## Features

- BFS and DFS crawling with depth and page limits, whole-word search and optional subdomains
- `robots.txt` support and per-domain request rate limiting
- Content cleaning pipeline: scripts, styles, embedded objects, ads, optional navigation removal
- Response cache with configurable TTL
- Pause and resume, including after an application restart (state snapshots in the database)
- Statistics and metadata of every visited page, computed with SQL
- Distributed crawling: an initiator hands out batches of URLs to peers, lost peers are detected by heartbeat and their batches are returned to the queue

## Tech stack

Java 17, JavaFX 17, Gradle 8 (wrapper), Jsoup, H2, Gson, JUnit 5.

## Build and run

JDK 17 is required.

```powershell
.\gradlew.bat clean test            # build and run the tests
.\gradlew.bat jpackageAppImage      # self-contained app image with WebCrawler.exe (Windows)
```

The packaged application does not need a separate Java installation. On start it creates `webcrawler.mv.db`, `results/` and `cache/` in the data directory. The directory is set with `--data-dir=<path>`; without it the working directory is used.

## Distributed mode

On one instance, enter a port (1024-65535, default 9090) in the "Розподілений обхід (P2P)" block and press "Створити розподілену сесію". On the other instances, enter the initiator's host and port and press "Підключитися". To run several instances on one computer, give each its own data directory (`--data-dir` or a copy of the application folder).

## Architecture

Six design patterns, five object-oriented and one architectural:

| Pattern | Where |
|---|---|
| Proxy | `CachingPageLoader` in front of `HttpPageLoader` (`PageLoader`) |
| Composite | `SiteElement`, `DirectoryElement`, `PageElement` |
| Chain of Responsibility | `ContentFilter` and five concrete filters |
| Memento | `CrawlSession`, `StateSnapshot`, `SnapshotManager` |
| Template Method | `CrawlStrategy`, `BreadthFirstCrawl`, `DepthFirstCrawl` |
| P2P (hybrid) | `SessionCoordinator`, `PeerServer`, `PeerClient` |

```
src/main/java/com/webcrawler   core, model, loader, filter, processing, storage,
                               persistence, p2p, ui, config, util
src/main/resources             FXML views, styles, db/schema.sql
src/test/java/com/webcrawler   unit and integration tests
```

Tests use stubbed loaders and a local HTTP server, so they do not touch the external network.

## Limitations

Only static HTML is parsed (no JavaScript execution), and search is not morphology-aware. The P2P protocol is meant for a trusted local network and has no authentication or encryption. If the initiator goes away, the distributed session ends.
