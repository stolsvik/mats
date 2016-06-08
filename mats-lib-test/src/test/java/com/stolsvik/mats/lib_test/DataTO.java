package com.stolsvik.mats.lib_test;

/**
 * A <i>Data Transfer Object</i> meant for unit tests.
 * <p>
 * Note about DTOs in general: The DTOs used in MATS endpoints are to be considered their public interface, and
 * should be documented thoroughly.
 */
public class DataTO {
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