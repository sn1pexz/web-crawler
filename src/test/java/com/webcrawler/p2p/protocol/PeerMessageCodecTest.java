package com.webcrawler.p2p.protocol;

import com.google.gson.JsonObject;
import com.webcrawler.filter.FilterType;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.StrategyType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PeerMessageCodecTest {

    private final PeerMessageCodec codec = new PeerMessageCodec();

    @Test
    void roundTripsAllTenMessageTypesAsSingleLineNdjson() {
        for (MessageType type : MessageType.values()) {
            JsonObject payload = codec.createPayload();
            payload.addProperty("note", "line1\nline2\r\n");
            PeerMessage original = new PeerMessage(type, "peer-42", "req-1", "OK", payload);

            String encoded = codec.encode(original);
            assertFalse(encoded.contains("\n"), "NDJSON must be single-line for " + type);
            assertFalse(encoded.contains("\r"), "NDJSON must not contain CR for " + type);

            PeerMessage decoded = codec.decode(encoded);
            assertEquals(original, decoded);
        }
    }

    @Test
    void roundTripsTypedCrawlSettingsQueueEntriesAndPagesWithLocalDateTime() {
        CrawlSettings settings = new CrawlSettings(
                "https://example.com/start",
                "пошук",
                StrategyType.DFS,
                3,
                250,
                true,
                true,
                EnumSet.of(FilterType.SCRIPT, FilterType.NAVIGATION));

        List<QueueEntry> entries = List.of(
                new QueueEntry("https://example.com/a", 1),
                new QueueEntry("https://example.com/b?x=1", 2));

        LocalDateTime timestamp = LocalDateTime.of(2026, 11, 19, 14, 30, 15, 123_000_000);
        Page page = new Page(
                10L,
                5L,
                2L,
                "https://example.com/a",
                "Заголовок сторінки",
                timestamp,
                2048L,
                1,
                200,
                null,
                true,
                "results/example.com/a.html");

        JsonObject payload = codec.createPayload();
        codec.putSettings(payload, settings);
        codec.putQueueEntries(payload, entries);
        codec.putPages(payload, List.of(page));
        payload.addProperty("activeWorkersCount", 3);

        PeerMessage message = new PeerMessage(MessageType.RESULT, "peer-1", "req-99", "OK", payload);
        String line = codec.encode(message);
        assertFalse(line.contains("\n"));

        PeerMessage decoded = codec.decode(line);
        JsonObject decodedPayload = decoded.getPayload();

        assertEquals(settings, codec.extractSettings(decodedPayload));
        assertEquals(entries, codec.extractQueueEntries(decodedPayload));
        assertEquals(List.of(page), codec.extractPages(decodedPayload));
        assertEquals(3, decodedPayload.get("activeWorkersCount").getAsInt());
    }
}
