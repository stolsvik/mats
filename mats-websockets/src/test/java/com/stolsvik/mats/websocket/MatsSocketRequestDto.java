package com.stolsvik.mats.websocket;

import java.util.Objects;

/**
 * Incoming class for MatsSocket Endpoint.
 */
public class MatsSocketRequestDto {
    public String string;
    public double number;
    public long requestTimestamp;

    @Override
    public int hashCode() {
        return string.hashCode() + (int) Double.doubleToLongBits(number * 99713.80309);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MatsSocketRequestDto)) {
            throw new AssertionError(MatsSocketRequestDto.class.getSimpleName() + " was attempted equalled to [" + obj + "].");
        }
        MatsSocketRequestDto other = (MatsSocketRequestDto) obj;
        return Objects.equals(this.string, other.string) && (this.number == other.number);
    }

    @Override
    public String toString() {
        return "MatsSocketRequestDto [string=" + string + ", number=" + number + "]";
    }
}