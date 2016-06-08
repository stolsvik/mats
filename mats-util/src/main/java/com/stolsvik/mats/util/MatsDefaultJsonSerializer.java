package com.stolsvik.mats.util;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stolsvik.mats.MatsTrace;

/**
 * Implementation of {@link MatsStringSerializer} that employs <a href="https://github.com/FasterXML/jackson">Jackson
 * JSON library</a> for serialization and deserialization.
 * <p>
 * Notice that there is a special case for Strings for {@link #serializeObject(Object)} and
 * {@link #deserializeObject(String, Class)}: For the serialize-method, if the value is a String, it is returned
 * directly ("serialized as-is"), while when deserialized and the requested class is String, the supplied "json" String
 * argument is returned directly. This enables an endpoint to receive any type of value if it specifies String.class as
 * expected DTO, as it'll just get the JSON document itself - and thus effectively acts as a Java method taking Object.
 * <p>
 * The Jackson ObjectMapper is configured to only handle fields (think "data struct"), i.e. not use setters or getters,
 * and upon deserialization to ignore properties from the JSON that has no field in the class to be deserialized into
 * (to handle <i>widening conversions<i> for incoming DTOs):
 *
 * <pre>
 * ObjectMapper mapper = new ObjectMapper();
 * mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
 * mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
 * mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class MatsDefaultJsonSerializer implements MatsStringSerializer {

    private static final ObjectMapper __objectMapper;

    static {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        __objectMapper = mapper;
    }

    @Override
    public String serializeMatsTrace(MatsTrace matsTrace) {
        try {
            return __objectMapper.writeValueAsString(matsTrace);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Couldn't serialize MatsTrace, which is crazy!\n" + matsTrace, e);
        }
    }

    @Override
    public MatsTrace deserializeMatsTrace(String json) {
        try {
            return __objectMapper.readValue(json, MatsTrace.class);
        }
        catch (IOException e) {
            throw new AssertionError("Couldn't deserialize MatsTrace from given JSON, which is crazy!\n" + json, e);
        }
    }

    @Override
    public String serializeObject(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return (String) object;
        }
        try {
            return __objectMapper.writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw new AssertionError("Couldn't serialize Object [" + object + "].", e);
        }
    }

    @Override
    public <T> T deserializeObject(String json, Class<T> type) {
        if (json == null) {
            return null;
        }
        if (type == String.class) {
            // Well, this is certainly pretty obviously correct.
            @SuppressWarnings("unchecked")
            T ret = (T) json;
            return ret;
        }
        try {
            return __objectMapper.readValue(json, type);
        }
        catch (IOException e) {
            throw new AssertionError("Couldn't deserialize JSON into object of type [" + type + "].\n" + json, e);
        }
    }
}
