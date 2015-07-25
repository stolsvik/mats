package com.stolsvik.mats.util;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stolsvik.mats.MatsTrace;

/**
 * Implementation of {@link MatsStringSerializer} that employs <a href="https://github.com/FasterXML/jackson">Jackson
 * JSON library</a> for serialization and deserialization.
 * <p>
 * The Jackson ObjectMapper is configured as such:
 *
 * <pre>
 * ObjectMapper mapper = new ObjectMapper();
 * mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
 * mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
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
