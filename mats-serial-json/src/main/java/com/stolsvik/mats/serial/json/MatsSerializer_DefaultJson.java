package com.stolsvik.mats.serial.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.KeepMatsTrace;
import com.stolsvik.mats.serial.impl.MatsTraceStringImpl;

/**
 * Implementation of {@link MatsSerializer} that employs <a href="https://github.com/FasterXML/jackson">Jackson
 * JSON library</a> for serialization and deserialization, and compress and decompress using {@link Deflater} and
 * {@link Inflater}.
 * <p>
 * Notice that there is a special case for Strings for {@link #serializeObject(Object)} and
 * {@link #deserializeObject(String, Class)}: For the serialize-method, if the value is a String, it is returned
 * directly ("serialized as-is"), while when deserialized and the requested class is String, the supplied "json" String
 * argument is returned directly. This enables an endpoint to receive any type of value if it specifies String.class as
 * expected DTO, as it'll just get the JSON document itself - and thus effectively acts as a Java method taking Object.
 * <p>
 * The Jackson {@link ObjectMapper} is configured to only handle fields (think "data struct"), i.e. not use setters or getters;
 * and to only include non-null fields; and upon deserialization to ignore properties from the JSON that has no field in
 * the class to be deserialized into (both to enable the modification of DTOs on the client side by removing fields
 * that aren't used in that client scenario, and to handle <i>widening conversions<i> for incoming DTOs):
 * <p>
 * <pre>
 * ObjectMapper mapper = new ObjectMapper();
 * mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
 * mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
 * mapper.setSerializationInclusion(Include.NON_NULL);
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
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        _matsTraceJson_Reader = mapper.readerFor(MatsTraceStringImpl.class);
        _matsTraceJson_Writer = mapper.writerFor(MatsTraceStringImpl.class);
        _objectMapper = mapper;
    }

    @Override
    public MatsTrace<String> createNewMatsTrace(String traceId, KeepMatsTrace keepMatsTrace, boolean nonPersistent, boolean interactive) {
        return MatsTraceStringImpl.createNew(traceId, keepMatsTrace, nonPersistent, interactive);
    }

    private String COMPRESS_DEFLATE = "deflate";
    private String COMPRESS_PLAIN = "plain";

    @Override
    public SerializedMatsTrace serializeMatsTrace(MatsTrace<String> matsTrace) {
        try {
            long nanosStart = System.nanoTime();
            byte[] serializedBytes = _matsTraceJson_Writer.writeValueAsBytes(matsTrace);
            long nanosAfterSerialization = System.nanoTime();
            double serializationMillis = (nanosAfterSerialization - nanosStart) / 1_000_000d;

            String meta;
            byte[] compressedBytes;
            double compressionMillis;

            if (serializedBytes.length > 600) {
                compressedBytes = compress(serializedBytes);
                compressionMillis = (System.nanoTime() - nanosAfterSerialization) / 1_000_000d;
                meta = COMPRESS_DEFLATE;
            }
            else {
                compressedBytes = serializedBytes;
                compressionMillis = 0d;
                meta = COMPRESS_PLAIN;
            }

            return new SerializedMatsTraceImpl(compressedBytes, meta, serializedBytes.length, serializationMillis, compressionMillis);
        }
        catch (JsonProcessingException e) {
            throw new SerializationException("Couldn't serialize MatsTrace, which is crazy!\n" + matsTrace, e);
        }
    }

    private static class SerializedMatsTraceImpl implements SerializedMatsTrace {
        private final byte[] _matsTraceBytes;
        private final String _meta;
        private final int _sizeUncompressed;
        private final double _millisSerialization;
        private final double _millisCompression;

        public SerializedMatsTraceImpl(byte[] matsTraceBytes, String meta, int sizeUncompressed, double millisSerialization, double millisCompression) {
            _matsTraceBytes = matsTraceBytes;
            _meta = meta;
            _sizeUncompressed = sizeUncompressed;
            _millisSerialization = millisSerialization;
            _millisCompression = millisCompression;
        }

        @Override
        public byte[] getMatsTraceBytes() {
            return _matsTraceBytes;
        }

        @Override
        public String getMeta() {
            return _meta;
        }

        @Override
        public int getSizeUncompressed() {
            return _sizeUncompressed;
        }

        @Override
        public double getMillisSerialization() {
            return _millisSerialization;
        }

        @Override
        public double getMillisCompression() {
            return _millisCompression;
        }
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public DeserializedMatsTrace<String> deserializeMatsTrace(byte[] matsTraceBytes, String meta) {
        try {
            long nanosStart = System.nanoTime();
            byte[] decompressedBytes;
            double decompressionMillis = 0;
            long nanosAfterDecompression;
            if (meta.startsWith(COMPRESS_DEFLATE)) {
                // -> Compressed, so decompress it
                decompressedBytes = decompress(matsTraceBytes);
                nanosAfterDecompression = System.nanoTime();
                decompressionMillis = (nanosAfterDecompression - nanosStart) / 1_000_000d;
            }
            else if (meta.startsWith(COMPRESS_PLAIN)) {
                // -> Plain, no compression - so just set the 'decompressedBytes' directly to the incoming bytes.
                decompressedBytes = matsTraceBytes;
                nanosAfterDecompression = nanosStart;
            }
            else {
                throw new AssertionError("Can only deserialize 'plain' and 'deflate'.");
            }

            MatsTrace<String> matsTrace = _matsTraceJson_Reader.readValue(decompressedBytes);
            double deserializationMillis = (System.nanoTime() - nanosAfterDecompression) / 1_000_000d;
            return new DeserializedMatsTraceImpl(matsTrace, decompressedBytes.length, deserializationMillis, decompressionMillis);
        }
        catch (IOException e) {
            throw new SerializationException("Couldn't deserialize MatsTrace from given JSON, which is crazy!\n"
                    + new String(matsTraceBytes, UTF8), e);
        }
    }

    private static final class DeserializedMatsTraceImpl implements DeserializedMatsTrace<String> {
        private final MatsTrace<String> _matsTrace;
        private final int _sizeUncompressed;
        private final double _millisDeserialization;
        private final double _millisDecompression;

        public DeserializedMatsTraceImpl(MatsTrace<String> matsTrace, int sizeUncompressed, double millisDeserialization, double millisDecompression) {
            _matsTrace = matsTrace;
            _sizeUncompressed = sizeUncompressed;
            _millisDeserialization = millisDeserialization;
            _millisDecompression = millisDecompression;
        }

        @Override
        public MatsTrace<String> getMatsTrace() {
            return _matsTrace;
        }

        @Override
        public int getSizeDecompressed() {
            return _sizeUncompressed;
        }

        @Override
        public double getMillisDeserialization() {
            return _millisDeserialization;
        }

        @Override
        public double getMillisDecompression() {
            return _millisDecompression;
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
            throw new CannotCreateEmptyInstanceException("Missing no-args constructor on class ["
                    + clazz.getName() + "].", e);
        }
        try {
            noArgsConstructor.setAccessible(true);
            return noArgsConstructor.newInstance();
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new CannotCreateEmptyInstanceException("Couldn't create new empty instance of class ["
                    + clazz.getName() + "].", e);
        }
    }

    private static class CannotCreateEmptyInstanceException extends SerializationException {
        CannotCreateEmptyInstanceException(String message, Throwable cause) {
            super(message, cause);
        }
    }


    protected byte[] compress(byte[] data)  {
        // OPTIMIZE: Use Object Pool for compressor-instances with Deflater and byte array.
        // This pool could possibly be a simple lock-free stack, if stack is empty, make a new instance.
        Deflater deflater = new Deflater();
        try {
            deflater.setInput(data);
            deflater.setLevel(Deflater.BEST_COMPRESSION);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
            deflater.finish();
            byte[] buffer = new byte[2048];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            try {
                outputStream.close();
            }
            catch (IOException e) {
                throw new AssertionError("Shall not throw IOException here.", e);
            }
            return outputStream.toByteArray();
        }
        finally {
            // Invoke the "end()" method to timely release off-heap resource, thus not depending on finalization.
            deflater.end();
        }
    }

    protected byte[] decompress(byte[] data) {
        // OPTIMIZE: Use Object Pool for decompressor-instances with Inflater and byte array.
        // This pool could possibly be a simple lock-free stack, if stack is empty, make a new instance.
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(data);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length * 10);
            byte[] buffer = new byte[4096];
            while (!inflater.finished()) {
                try {
                    int count = inflater.inflate(buffer);
                    outputStream.write(buffer, 0, count);
                }
                catch (DataFormatException e) {
                    throw new DecompressionException("DataFormatException was bad here.", e);
                }
            }
            try {
                outputStream.close();
            }
            catch (IOException e) {
                throw new AssertionError("Shall not throw IOException here.", e);
            }
            return outputStream.toByteArray();
        }
        finally {
            // Invoke the "end()" method to timely release off-heap resource, thus not depending on finalization.
            inflater.end();
        }
    }

    private static class DecompressionException extends SerializationException {
        DecompressionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
