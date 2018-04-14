package com.stolsvik.mats.serial.json;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;

/**
 * Implementation of {@link MatsSerializer} that employs <a href="https://github.com/FasterXML/jackson">Jackson
 * JSON library</a> for serialization and deserialization.
 * <p>
 * Notice that there is a special case for Strings for {@link #serializeObject(Object)} and
 * {@link #deserializeObject(String, Class)}: For the serialize-method, if the value is a String, it is returned
 * directly ("serialized as-is"), while when deserialized and the requested class is String, the supplied "json" String
 * argument is returned directly. This enables an endpoint to receive any type of value if it specifies String.class as
 * expected DTO, as it'll just get the JSON document itself - and thus effectively acts as a Java method taking Object.
 * (Also note that the ordinary serialization process for JSON also directly returns the toString()'ed values of any
 * {@link Number} or {@link Boolean}.)
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
public class MatsSerializer_DefaultJson implements MatsSerializer<String> {

    private final ObjectMapper _objectMapper;
    private final ObjectReader _matsTraceJson_Reader;
    private final ObjectWriter _matsTraceJson_Writer;

    public MatsSerializer_DefaultJson() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        _matsTraceJson_Reader = mapper.readerFor(MatsTraceImpl.class);
        _matsTraceJson_Writer = mapper.writerFor(MatsTraceImpl.class);
        _objectMapper = mapper;
    }

    @Override
    public MatsTrace<String> createNewMatsTrace(String traceId, boolean keepTrace, boolean nonPersistent, boolean interactive) {
        return MatsTraceImpl.createNew(traceId, keepTrace, nonPersistent, interactive);
    }

    @Override
    public byte[] serializeMatsTrace(MatsTrace<String> matsTrace) {
        try {
            return _matsTraceJson_Writer.writeValueAsBytes(matsTrace);
        }
        catch (JsonProcessingException e) {
            throw new SerializationException("Couldn't serialize MatsTrace, which is crazy!\n" + matsTrace, e);
        }
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public MatsTrace<String> deserializeMatsTrace(byte[] jsonUtf8ByteArray) {
        try {
            return _matsTraceJson_Reader.readValue(jsonUtf8ByteArray);
        }
        catch (IOException e) {
            throw new SerializationException("Couldn't deserialize MatsTrace from given JSON, which is crazy!\n"
                    + new String(jsonUtf8ByteArray, UTF8), e);
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
            return _objectMapper.writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw new SerializationException("Couldn't serialize Object [" + object + "].", e);
        }
    }

    @Override
    public <T> T deserializeObject(String serialized, Class<T> type) {
        if (serialized == null) {
            return null;
        }
        if (type == String.class) {
            // Well, this is certainly pretty obviously correct.
            @SuppressWarnings("unchecked")
            T ret = (T) serialized;
            return ret;
        }
        try {
            return _objectMapper.readValue(serialized, type);
        }
        catch (IOException e) {
            throw new SerializationException("Couldn't deserialize JSON into object of type [" + type + "].\n"
                    + serialized, e);
        }
    }

    @Override
    public <T> T newInstance(Class<T> clazz) {
        Constructor<T> noArgsConstructor;
        try {
            noArgsConstructor = clazz.getDeclaredConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new CannotCreateEmptyInstanceException("Missing no-args constructor on STO class ["
                    + clazz.getName() + "].", e);
        }
        try {
            noArgsConstructor.setAccessible(true);
            return noArgsConstructor.newInstance();
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new CannotCreateEmptyInstanceException("Couldn't create new empty instance of STO class ["
                    + clazz.getName() + "].", e);
        }
    }

    private static class CannotCreateEmptyInstanceException extends SerializationException {
        public CannotCreateEmptyInstanceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
