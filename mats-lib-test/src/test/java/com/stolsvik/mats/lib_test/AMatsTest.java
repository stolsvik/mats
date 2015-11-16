package com.stolsvik.mats.lib_test;

import java.util.UUID;

import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.Rule_Mats;

public class AMatsTest {
    protected Logger log = LoggerFactory.getLogger(getClass());

    protected String INITIATOR = getClass().getName() + ".INITIATOR";
    protected String SERVICE = getClass().getName() + ".SERVICE";
    protected String TERMINATOR = getClass().getName() + ".TERMINATOR";

    {
        log.info("### Instantiating class [" + this.getClass().getName() + "].");
    }

    @Rule
    public Rule_Mats matsRule = new Rule_Mats();

    protected MatsTestLatch matsTestLatch = new MatsTestLatch();

    /**
     * @return a random UUID string, to be used for traceIds when testing - <b>ALWAYS use a semantically meaningful,
     *         globally unique Id as traceId in production code!</b>
     */
    protected String randomId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Sleeps the specified number of milliseconds - can emulate processing time, primarily meant for the concurrency
     * tests.
     *
     * @param millis
     *            the number of millis to sleep
     * @throws AssertionError
     *             if an {@link InterruptedException} occurs.
     */
    protected void sleep(int millis) throws AssertionError {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Testing State Transfer Object.
     * <p>
     * Note about STOs in general: The STOs in use by the different multi-stage endpoints are to be considered private
     * implementation details, and should typically reside as a private inner class of the endpoint employing them.
     * <p>
     * The possibility to send STOs along with the request should only be employed if both the sender and the endpoint
     * to which the state is sent along to, resides in the same code base, and hence still can be considered private
     * implementation details.
     */
    public static class StateTO {
        public int number1;
        public double number2;

        public StateTO() {
            // For Jackson JSON-lib which needs default constructor.
        }

        public StateTO(int number1, double number2) {
            this.number1 = number1;
            this.number2 = number2;
        }

        @Override
        public int hashCode() {
            return (number1 * 3539) + (int) Double.doubleToLongBits(number2 * 99713.80309);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof StateTO)) {
                throw new AssertionError(StateTO.class.getSimpleName() + " was attempted equalled to [" + obj + "].");
            }
            StateTO other = (StateTO) obj;
            return (this.number1 == other.number1) && (this.number2 == other.number2);
        }

        @Override
        public String toString() {
            return "StateTO [number1=" + number1 + ", number2=" + number2 + "]";
        }
    }

    /**
     * Testing Data Transfer Object.
     * <p>
     * Note about DTOs in general: The DTOs are to be considered the public interface of a MATS endpoint, and should be
     * documented thoroughly.
     */
    public static class DataTO {
        public double number;
        public String string;

        public DataTO() {
            // For Jackson JSON-lib which needs default constructor.
        }

        public DataTO(double number, String string) {
            this.number = number;
            this.string = string;
        }

        @Override
        public int hashCode() {
            return ((int) Double.doubleToLongBits(number) * 4549) + (string != null ? string.hashCode() : 0);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof DataTO)) {
                throw new AssertionError(DataTO.class.getSimpleName() + " was attempted equalled to [" + obj + "].");
            }
            DataTO other = (DataTO) obj;
            if ((this.string == null) ^ (other.string == null)) {
                return false;
            }
            return (this.number == other.number) && ((this.string == null) || (this.string.equals(other.string)));
        }

        @Override
        public String toString() {
            return "DataTO [number=" + number + ", string=" + string + "]";
        }
    }
}
