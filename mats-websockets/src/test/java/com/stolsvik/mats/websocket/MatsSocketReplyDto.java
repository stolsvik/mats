package com.stolsvik.mats.websocket;

/**
 * Incoming class for MatsSocket Endpoint.
 */
public class MatsSocketReplyDto {
    public int number1;
    public double number2;
    public long requestTimestamp;

    public MatsSocketReplyDto() {
    }

    public MatsSocketReplyDto(int number1, double number2, long requestTimestamp) {
        this.number1 = number1;
        this.number2 = number2;
        this.requestTimestamp = requestTimestamp;
    }

    @Override
    public int hashCode() {
        return (number1 * 3539) + (int) Double.doubleToLongBits(number2 * 99713.80309);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MatsSocketReplyDto)) {
            throw new AssertionError(MatsSocketReplyDto.class.getSimpleName() + " was attempted equalled to [" + obj + "].");
        }
        MatsSocketReplyDto other = (MatsSocketReplyDto) obj;
        return (this.number1 == other.number1) && (this.number2 == other.number2);
    }

    @Override
    public String toString() {
        return "MatsSocketReplyDto [number1=" + number1 + ", number2=" + number2 + "]";
    }
}