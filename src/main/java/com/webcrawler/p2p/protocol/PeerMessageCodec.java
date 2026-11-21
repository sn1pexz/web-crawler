package com.webcrawler.p2p.protocol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.Page;
import com.webcrawler.model.QueueEntry;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

public final class PeerMessageCodec {

    public static final String KEY_SETTINGS = "settings";
    public static final String KEY_ENTRIES = "entries";
    public static final String KEY_PAGES = "pages";

    private static final Type QUEUE_ENTRY_LIST_TYPE = new TypeToken<List<QueueEntry>>() { }.getType();
    private static final Type PAGE_LIST_TYPE = new TypeToken<List<Page>>() { }.getType();

    private final Gson gson;

    public PeerMessageCodec() {
        this.gson = new GsonBuilder()
                .disableHtmlEscaping()
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
                .create();
    }

    public String encode(PeerMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        if (message.getType() == null) {
            throw new IllegalArgumentException("message.type must not be null");
        }
        return gson.toJson(message);
    }

    public PeerMessage decode(String jsonLine) {
        if (jsonLine == null || jsonLine.isBlank()) {
            throw new IllegalArgumentException("jsonLine must not be blank");
        }
        PeerMessage decoded = gson.fromJson(jsonLine, PeerMessage.class);
        if (decoded == null || decoded.getType() == null) {
            throw new IllegalArgumentException("Decoded PeerMessage is missing required 'type' field");
        }
        return decoded;
    }

    public Gson getGson() {
        return gson;
    }

    public JsonObject createPayload() {
        return new JsonObject();
    }

    public JsonObject encodeSettingsPayload(CrawlSettings settings) {
        JsonObject payload = new JsonObject();
        putSettings(payload, settings);
        return payload;
    }

    public void putSettings(JsonObject payload, CrawlSettings settings) {
        putValue(payload, KEY_SETTINGS, Objects.requireNonNull(settings, "settings must not be null"));
    }

    public CrawlSettings extractSettings(JsonObject payload) {
        if (payload == null) {
            return null;
        }
        if (payload.has(KEY_SETTINGS)) {
            return extractValue(payload, KEY_SETTINGS, CrawlSettings.class);
        }
        return gson.fromJson(payload, CrawlSettings.class);
    }

    public JsonObject encodeQueueEntriesPayload(List<QueueEntry> entries) {
        JsonObject payload = new JsonObject();
        putQueueEntries(payload, entries);
        return payload;
    }

    public void putQueueEntries(JsonObject payload, List<QueueEntry> entries) {
        putQueueEntries(payload, KEY_ENTRIES, entries);
    }

    public void putQueueEntries(JsonObject payload, String key, List<QueueEntry> entries) {
        putValue(payload, key, entries == null ? List.of() : entries);
    }

    public List<QueueEntry> extractQueueEntries(JsonObject payload) {
        return extractQueueEntries(payload, KEY_ENTRIES);
    }

    public List<QueueEntry> extractQueueEntries(JsonObject payload, String key) {
        List<QueueEntry> list = extractValue(payload, key, QUEUE_ENTRY_LIST_TYPE);
        return list == null ? List.of() : List.copyOf(list);
    }

    public JsonObject encodePagesPayload(List<Page> pages) {
        JsonObject payload = new JsonObject();
        putPages(payload, pages);
        return payload;
    }

    public void putPages(JsonObject payload, List<Page> pages) {
        putPages(payload, KEY_PAGES, pages);
    }

    public void putPages(JsonObject payload, String key, List<Page> pages) {
        putValue(payload, key, pages == null ? List.of() : pages);
    }

    public List<Page> extractPages(JsonObject payload) {
        return extractPages(payload, KEY_PAGES);
    }

    public List<Page> extractPages(JsonObject payload, String key) {
        List<Page> list = extractValue(payload, key, PAGE_LIST_TYPE);
        return list == null ? List.of() : List.copyOf(list);
    }

    public <T> void putValue(JsonObject payload, String key, T value) {
        Objects.requireNonNull(payload, "payload must not be null");
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        JsonElement tree = gson.toJsonTree(value);
        payload.add(key, tree);
    }

    public <T> T extractValue(JsonObject payload, String key, Type type) {
        if (payload == null || key == null || !payload.has(key) || payload.get(key).isJsonNull()) {
            return null;
        }
        return gson.fromJson(payload.get(key), type);
    }

    private static final class LocalDateTimeAdapter extends TypeAdapter<LocalDateTime> {

        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

        @Override
        public void write(JsonWriter out, LocalDateTime value) throws IOException {
            if (value == null) {
                out.nullValue();
            } else {
                out.value(value.format(FORMATTER));
            }
        }

        @Override
        public LocalDateTime read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            return LocalDateTime.parse(in.nextString(), FORMATTER);
        }
    }
}
