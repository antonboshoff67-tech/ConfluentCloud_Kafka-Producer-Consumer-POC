package com.antontech.itemkafka_poc.kafka.consumer;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Gson {@link JsonSerializer}/{@link JsonDeserializer} for {@link LocalDateTime}
 * using the ISO-8601 local date-time format. Registered on every Gson
 * instance used across the producer/consumer services and Flink jobs so
 * that {@code createDte}/{@code lastUpdateDte} fields on
 * {@link com.antontech.itemkafka_poc.model.Item} round-trip correctly
 * through JSON.
 */
public class LocalDateTimeAdapter implements JsonSerializer<LocalDateTime>, JsonDeserializer<LocalDateTime> {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME; // Use ISO format

    /**
     * @param dateTime  the value to serialize, may be {@code null}.
     * @param typeOfSrc unused (required by the Gson SPI).
     * @param context   unused (required by the Gson SPI).
     * @return a JSON string primitive in ISO-8601 format, or JSON {@code null} if {@code dateTime} is {@code null}.
     */
    @Override
    public JsonElement serialize(LocalDateTime dateTime, Type typeOfSrc, JsonSerializationContext context) {
        return dateTime != null ? new JsonPrimitive(dateTime.format(formatter)) : null; // Serialize to JSON format
    }

    /**
     * @param json    the JSON element to deserialize, expected to be an ISO-8601 date-time string.
     * @param typeOfT unused (required by the Gson SPI).
     * @param context unused (required by the Gson SPI).
     * @return the parsed {@link LocalDateTime}.
     * @throws JsonParseException if the string cannot be parsed as an ISO-8601 local date-time.
     */
    @Override
    public LocalDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        return LocalDateTime.parse(json.getAsString(), formatter); // Deserialize back to LocalDateTime
    }
}
