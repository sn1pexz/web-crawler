package com.webcrawler.core;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.webcrawler.filter.FilterType;
import com.webcrawler.model.CrawlSettings;
import com.webcrawler.model.QueueEntry;
import com.webcrawler.model.SessionStatus;
import com.webcrawler.model.StateSnapshot;
import com.webcrawler.model.StrategyType;
import com.webcrawler.persistence.DatabaseManager;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

//Memento caretaker handling JSON snapshot persistence
public class SnapshotManager {

    private static final Type QUEUE_LIST_TYPE = new TypeToken<List<QueueEntry>>() {
    }.getType();
    private static final Type COUNTERS_MAP_TYPE = new TypeToken<Map<String, Integer>>() {
    }.getType();

    private final DatabaseManager databaseManager;
    private final Gson gson;

    public SnapshotManager(DatabaseManager databaseManager) {
        this.databaseManager = Objects.requireNonNull(databaseManager, "databaseManager must not be null");
        this.gson = createGson();
    }

    public void save(StateSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        if (snapshot.getSessionId() <= 0) {
            throw new IllegalStateException("Не вдалося зберегти знімок стану: некоректний ідентифікатор сесії.");
        }
        CrawlSettings settings = snapshot.getSettings();
        if (settings == null || !settings.validate()) {
            throw new IllegalStateException("Не вдалося зберегти знімок стану: некоректні налаштування обходу.");
        }

        String settingsJson = gson.toJson(settings);
        String queueJson = gson.toJson(snapshot.getQueue(), QUEUE_LIST_TYPE);
        String countersJson = gson.toJson(snapshot.getCounters(), COUNTERS_MAP_TYPE);

        Map<String, Integer> counters = snapshot.getCounters();
        int visited = counters.getOrDefault(StateSnapshot.COUNTER_VISITED, 0);
        int matched = counters.getOrDefault(StateSnapshot.COUNTER_MATCHED, 0);
        int errors = counters.getOrDefault(StateSnapshot.COUNTER_ERRORS, 0);

        String updateSessionSql = """
                UPDATE crawl_sessions
                SET status = ?,
                    completion_reason = NULL,
                    visited_count = ?,
                    matched_count = ?,
                    error_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """;

        String mergeSnapshotSql = """
                MERGE INTO state_snapshots (session_id, settings_json, queue_json, counters_json, saved_at)
                KEY (session_id)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """;

        try {
            databaseManager.executeInTransaction(conn -> {
                try (PreparedStatement updateStmt = conn.prepareStatement(updateSessionSql)) {
                    updateStmt.setString(1, SessionStatus.PAUSED.name());
                    updateStmt.setInt(2, visited);
                    updateStmt.setInt(3, matched);
                    updateStmt.setInt(4, errors);
                    updateStmt.setLong(5, snapshot.getSessionId());
                    int updated = updateStmt.executeUpdate();
                    if (updated == 0) {
                        throw new MissingSessionException(snapshot.getSessionId());
                    }
                }

                try (PreparedStatement mergeStmt = conn.prepareStatement(mergeSnapshotSql)) {
                    mergeStmt.setLong(1, snapshot.getSessionId());
                    mergeStmt.setString(2, settingsJson);
                    mergeStmt.setString(3, queueJson);
                    mergeStmt.setString(4, countersJson);
                    mergeStmt.executeUpdate();
                }
                return null;
            });
        } catch (MissingSessionException ex) {
            throw new IllegalStateException(
                    "Не вдалося зберегти знімок стану: сесію #" + snapshot.getSessionId() + " не знайдено.", ex);
        } catch (RuntimeException ex) {
            throw new IllegalStateException(
                    "Не вдалося зберегти знімок стану сесії #" + snapshot.getSessionId() + " у базу даних.", ex);
        }
    }

    public List<Long> getPausedSessionIds() {
        String sql = """
                SELECT cs.id
                FROM crawl_sessions cs
                INNER JOIN state_snapshots ss ON ss.session_id = cs.id
                WHERE cs.status = ?
                ORDER BY ss.saved_at DESC, cs.id DESC
                """;
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, SessionStatus.PAUSED.name());
            try (ResultSet rs = stmt.executeQuery()) {
                List<Long> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getLong("id"));
                }
                return Collections.unmodifiableList(ids);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Не вдалося завантажити список призупинених сесій.", ex);
        }
    }

    public StateSnapshot load(long sessionId) {
        String sql = """
                SELECT settings_json, queue_json, counters_json
                FROM state_snapshots
                WHERE session_id = ?
                """;

        String settingsJson;
        String queueJson;
        String countersJson;
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, sessionId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Знімок стану для сесії #" + sessionId + " не знайдено.");
                }
                settingsJson = rs.getString("settings_json");
                queueJson = rs.getString("queue_json");
                countersJson = rs.getString("counters_json");
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Помилка читання знімка стану сесії #" + sessionId + " з бази даних.", ex);
        }

        try {
            CrawlSettings settings = deserializeSettings(settingsJson);
            List<QueueEntry> queue = deserializeQueue(queueJson);
            Map<String, Integer> counters = deserializeCounters(countersJson);
            return new StateSnapshot(sessionId, settings, queue, counters);
        } catch (RuntimeException ex) {
            throw new IllegalStateException(
                    "Знімок стану сесії #" + sessionId + " пошкоджено та не може бути відновлений.", ex);
        }
    }

    private CrawlSettings deserializeSettings(String settingsJson) {
        JsonElement element = parseNonBlankJson(settingsJson, "settings");
        if (!element.isJsonObject()) {
            throw new IllegalArgumentException("Settings JSON must be an object");
        }
        CrawlSettings settings = gson.fromJson(element, CrawlSettings.class);
        if (settings == null || !settings.validate()) {
            throw new IllegalArgumentException("Deserialized CrawlSettings failed validation");
        }
        return settings;
    }

    private List<QueueEntry> deserializeQueue(String queueJson) {
        JsonElement element = parseNonBlankJson(queueJson, "queue");
        if (!element.isJsonArray()) {
            throw new IllegalArgumentException("Queue JSON must be an array");
        }
        List<QueueEntry> queue = gson.fromJson(element, QUEUE_LIST_TYPE);
        if (queue == null) {
            throw new IllegalArgumentException("Queue JSON deserialized to null");
        }
        for (QueueEntry entry : queue) {
            if (entry == null || entry.getUrl() == null || entry.getUrl().isBlank() || entry.getDepth() < 0) {
                throw new IllegalArgumentException("Invalid queue entry in snapshot: " + entry);
            }
        }
        return queue;
    }

    private Map<String, Integer> deserializeCounters(String countersJson) {
        JsonElement element = parseNonBlankJson(countersJson, "counters");
        if (!element.isJsonObject()) {
            throw new IllegalArgumentException("Counters JSON must be an object");
        }
        Map<String, Integer> counters = gson.fromJson(element, COUNTERS_MAP_TYPE);
        if (counters == null) {
            throw new IllegalArgumentException("Counters JSON deserialized to null");
        }
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null || entry.getValue() < 0) {
                throw new IllegalArgumentException("Invalid counter entry in snapshot: " + entry);
            }
        }
        return counters;
    }

    private static JsonElement parseNonBlankJson(String json, String fieldName) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("Snapshot field '" + fieldName + "' is blank");
        }
        return JsonParser.parseString(json);
    }

    private static Gson createGson() {
        return new GsonBuilder()
                .registerTypeAdapter(StrategyType.class, strictEnumAdapter(StrategyType.class, true))
                .registerTypeAdapter(FilterType.class, strictEnumAdapter(FilterType.class, false))
                .create();
    }

    private static <E extends Enum<E>> TypeAdapter<E> strictEnumAdapter(Class<E> enumClass, boolean allowNull) {
        return new TypeAdapter<>() {
            @Override
            public void write(JsonWriter out, E value) throws IOException {
                if (value == null) {
                    out.nullValue();
                } else {
                    out.value(value.name());
                }
            }

            @Override
            public E read(JsonReader in) throws IOException {
                if (in.peek() == JsonToken.NULL) {
                    if (!allowNull) {
                        throw new JsonParseException("Null value not allowed for " + enumClass.getSimpleName());
                    }
                    in.nextNull();
                    return null;
                }
                if (in.peek() != JsonToken.STRING) {
                    throw new JsonParseException("Expected string for " + enumClass.getSimpleName());
                }
                String value = in.nextString();
                try {
                    return Enum.valueOf(enumClass, value);
                } catch (IllegalArgumentException ex) {
                    throw new JsonParseException("Unknown " + enumClass.getSimpleName() + ": " + value, ex);
                }
            }
        };
    }

    private static final class MissingSessionException extends RuntimeException {
        private MissingSessionException(long sessionId) {
            super("Session not found: " + sessionId);
        }
    }
}
