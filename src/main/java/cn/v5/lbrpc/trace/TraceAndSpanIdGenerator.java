package cn.v5.lbrpc.trace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * Created by yangwei on 19/9/16.
 */
public class TraceAndSpanIdGenerator {
    private static final Logger logger = LoggerFactory.getLogger(TraceAndSpanIdGenerator.class);

    private static final Random random = getRandomInstance("SHA1PRNG");

    public static String generateId() {
        byte[] random8Bytes = new byte[8];
        random.nextBytes(random8Bytes);

        long randomLong = convertBytesToLong(random8Bytes);

        return String.valueOf(randomLong);
    }

    /**
     * Converts the given 8 bytes to a long value. Implementation for this taken from {@link java.util.UUID#UUID(byte[])}.
     */
    protected static long convertBytesToLong(byte[] byteArray) {
        if (byteArray.length != 8)
            throw new IllegalArgumentException("byteArray must be 8 bytes in length");

        long longVal = 0;
        for (int i=0; i<8; i++)
            longVal = (longVal << 8) | (byteArray[i] & 0xff);

        return longVal;
    }

    /**
     * Tries to retrieve and return the {@link SecureRandom} with the given implementation using {@link SecureRandom#getInstance(String)}, and falls back to a
     * {@code new Random(System.nanoTime())} if that instance could not be found.
     */
    protected static Random getRandomInstance(String desiredSecureRandomImplementation) {
        Random randomToUse;

        try {
            randomToUse = SecureRandom.getInstance(desiredSecureRandomImplementation);
            randomToUse.setSeed(System.nanoTime());
        } catch (NoSuchAlgorithmException e) {
            logger.error("Unable to retrieve the {} SecureRandom instance. Defaulting to a new Random(System.nanoTime()) instead. NOTE: This means random longs will not cover " +
                    "the full 64 bits of possible values! See the javadocs for Random.nextLong() for details. dtracer_error=true", desiredSecureRandomImplementation,  e);
            randomToUse = new Random(System.nanoTime());
        }

        return randomToUse;
    }
}
