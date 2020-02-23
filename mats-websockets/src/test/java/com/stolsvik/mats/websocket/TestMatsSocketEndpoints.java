package com.stolsvik.mats.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpoint;

import java.util.Objects;

/**
 * Sets up the test endpoints used from the integration tests (and the HTML test-pages).
 *
 * @author Endre Stølsvik 2020-02-20 18:33 - http://stolsvik.com/, endre@stolsvik.com
 */
public class TestMatsSocketEndpoints {
    private static final Logger log = LoggerFactory.getLogger(TestMatsSocketEndpoints.class);

    static void setupMatsSocketEndpoints(MatsSocketServer matsSocketServer) {
        setupStandardTestSingle(matsSocketServer);
        setupResolveInIncoming(matsSocketServer);
        setupRejectInIncoming(matsSocketServer);
        setupThrowsInIncoming(matsSocketServer);
        setupResolveInReplyAdapter(matsSocketServer);
        setupRejectInReplyAdapter(matsSocketServer);
        setupThrowsInReplyAdapter(matsSocketServer);
    }

    private static final String STANDARD_ENDPOINT = "Test.single";

    private static void setupStandardTestSingle(MatsSocketServer matsSocketServer) {
        // :: Make default MatsSocket Endpoint
        MatsSocketEndpoint<MatsSocketRequestDto, MatsDataTO, MatsDataTO, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint(STANDARD_ENDPOINT,
                        MatsSocketRequestDto.class, MatsDataTO.class, MatsDataTO.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> {
                            log.info("Got MatsSocket request on MatsSocket EndpointId: " +
                                    ctx.getMatsSocketEndpointId());
                            log.info(" \\- Authorization: " + ctx.getAuthorizationHeader());
                            log.info(" \\- Principal:     " + ctx.getPrincipal());
                            log.info(" \\- UserId:        " + ctx.getUserId());
                            log.info(" \\- Message:       " + msIncoming);
                            ctx.forwardCustom(new MatsDataTO(msIncoming.number, msIncoming.string),
                                    msg -> msg.to(ctx.getMatsSocketEndpointId())
                                            .interactive()
                                            .nonPersistent()
                                            .setTraceProperty("requestTimestamp", msIncoming.requestTimestamp));
                        });

        // .. add the optional ReplyAdapter, needed here due to differing ReplyDTO between Mats and MatsSocket
        matsSocketEndpoint.replyAdapter((ctx, matsReply) -> {
            log.info("Adapting message: " + matsReply);
            MatsSocketReplyDto reply = new MatsSocketReplyDto(matsReply.string.length(), matsReply.number,
                    ctx.getMatsContext().getTraceProperty("requestTimestamp", Long.class));
            ctx.resolve(reply);
        });
    }

    private static void setupResolveInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketEndpoint("Test.resolveInIncomingHandler",
                MatsSocketRequestDto.class, Void.class, Void.class, MatsSocketReplyDto.class,
                // RESOLVE
                (ctx, principal, msIncoming) -> ctx.resolve(
                        new MatsSocketReplyDto(1, 2, msIncoming.requestTimestamp)));
    }

    private static void setupRejectInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketEndpoint("Test.rejectInIncomingHandler",
                MatsSocketRequestDto.class, Void.class, Void.class, MatsSocketReplyDto.class,
                // REJECT
                (ctx, principal, msIncoming) -> ctx.reject(
                        new MatsSocketReplyDto(3, 4, msIncoming.requestTimestamp)));
    }

    private static void setupThrowsInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketEndpoint("Test.throwsInIncomingHandler",
                MatsSocketRequestDto.class, Void.class, Void.class, MatsSocketReplyDto.class,
                // THROW
                (ctx, principal, msIncoming) -> {
                    throw new IllegalStateException("Exception in IncomingAuthorizationAndAdapter should REJECT");
                });
    }

    private static void setupResolveInReplyAdapter(MatsSocketServer matsSocketServer) {
        MatsSocketEndpoint<MatsSocketRequestDto, MatsDataTO, MatsDataTO, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint("Test.resolveInReplyAdapter",
                        MatsSocketRequestDto.class, MatsDataTO.class, MatsDataTO.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> ctx.forwardCustom(new MatsDataTO(1, "string1"),
                                msg -> msg.to(STANDARD_ENDPOINT)));
        // RESOLVE
        matsSocketEndpoint.replyAdapter((ctx, matsReply) -> ctx.resolve(new MatsSocketReplyDto(1, 2, 123)));
    }

    private static void setupRejectInReplyAdapter(MatsSocketServer matsSocketServer) {
        MatsSocketEndpoint<MatsSocketRequestDto, MatsDataTO, MatsDataTO, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint("Test.rejectInReplyAdapter",
                        MatsSocketRequestDto.class, MatsDataTO.class, MatsDataTO.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> ctx.forwardCustom(new MatsDataTO(2, "string2"),
                                msg -> msg.to(STANDARD_ENDPOINT)));
        // REJECT
        matsSocketEndpoint.replyAdapter((ctx, matsReply) -> ctx.reject(new MatsSocketReplyDto(1, 2, 123)));
    }

    private static void setupThrowsInReplyAdapter(MatsSocketServer matsSocketServer) {
        MatsSocketEndpoint<MatsSocketRequestDto, MatsDataTO, MatsDataTO, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint("Test.throwsInReplyAdapter",
                        MatsSocketRequestDto.class, MatsDataTO.class, MatsDataTO.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> ctx.forwardCustom(new MatsDataTO(3, "string3"),
                                msg -> msg.to(STANDARD_ENDPOINT)));
        // THROW
        matsSocketEndpoint.replyAdapter((ctx, matsReply) -> {
            throw new IllegalStateException("Exception in ReplyAdapter should REJECT.");
        });
    }

    /**
     * Request DTO class for MatsSocket Endpoint.
     */
    public static class MatsSocketRequestDto {
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

    /**
     * A DTO for Mats-side endpoint.
     */
    public static class MatsDataTO {
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

    /**
     * Reply DTO class for MatsSocket Endpoint.
     */
    public static class MatsSocketReplyDto {
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
}
