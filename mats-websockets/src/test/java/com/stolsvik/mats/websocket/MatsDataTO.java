package com.stolsvik.mats.websocket;

import java.util.Objects;

/**
 * A DTO for Mats-side endpoint.
 */
public class MatsDataTO {
    public double number;
    public String string;
    public int multiplier;

    public MatsDataTO() {
    }

    public MatsDataTO(double number, String string) {
        this.number = number;
        this.string = string;
    }

    public MatsDataTO(double number, String string, int multiplier) {
        this.number = number;
        this.string = string;
        this.multiplier = multiplier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        // NOTICE: Not Class-equals, but "instanceof", since we accept the "SubDataTO" too.
        if (o == null || !(o instanceof MatsDataTO)) return false;
        MatsDataTO matsDataTO = (MatsDataTO) o;
        return Double.compare(matsDataTO.number, number) == 0 &&
                multiplier == matsDataTO.multiplier &&
                Objects.equals(string, matsDataTO.string);
    }

    @Override
    public int hashCode() {
        return Objects.hash(number, string, multiplier);
    }

    @Override
    public String toString() {
        return "MatsDataTO [number=" + number
                + ", string=" + string
                + (multiplier != 0 ? ", multiplier="+multiplier : "")
                + "]";
    }
}