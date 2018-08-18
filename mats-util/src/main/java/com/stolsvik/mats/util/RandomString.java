package com.stolsvik.mats.util;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * If you need a random string for <u>a part of the traceId</u> (Read NOTE!), use this class instead of {@link UUID},
 * because UUID has low entropy density with only 4 bits per character, and dashes.
 */
public class RandomString {

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    /**
     * @return a 16-char length string, which should be plenty enough uniqueness for pretty much anything - you'll
     * have to make <i>extremely</i> many such strings to get a collision.
     */
    public static String randomCorrelationId() {
        return randomString(16);
    }

    /**
     * @return a 6-char length string, which should be enough to make an already-very-unique TraceId become unique
     * enough to be pretty sure that you will not ever have problems uniquely identifying log lines for a call flow.
     */
    public static String partTraceId() {
        return randomString(6);
    }

    /**
     * @param length the desired length of the returned random string.
     * @return a random string of the specified length.
     */
    public static String randomString(int length) {
        StringBuilder buf = new StringBuilder(length);
        for( int i = 0; i < length; i++ )
            buf.append( ALPHABET.charAt( ThreadLocalRandom.current().nextInt(ALPHABET.length()) ) );
        return buf.toString();
    }


}
