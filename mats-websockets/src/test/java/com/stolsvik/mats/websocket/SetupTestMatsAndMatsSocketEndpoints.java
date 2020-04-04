package com.stolsvik.mats.websocket;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.websocket.DummySessionAuthenticator.DummyAuthPrincipal;

/**
 * Sets up the test endpoints used from the integration tests (and the HTML test-pages).
 *
 * @author Endre Stølsvik 2020-02-20 18:33 - http://stolsvik.com/, endre@stolsvik.com
 */
public class SetupTestMatsAndMatsSocketEndpoints {
    private static final Logger log = LoggerFactory.getLogger(SetupTestMatsAndMatsSocketEndpoints.class);

    static void setupMatsAndMatsSocketEndpoints(MatsFactory matsFactory, MatsSocketServer matsSocketServer) {
        // "Standard" test endpoint
        setup_StandardTestSingle(matsSocketServer, matsFactory);
        setup_SimpleMats(matsSocketServer, matsFactory);

        // Resolve/Reject/Throws in incomingHandler and replyAdapter
        setupSocket_IgnoreInIncoming(matsSocketServer);
        setupSocket_DenyInIncoming(matsSocketServer);
        setupSocket_ResolveInIncoming(matsSocketServer);
        setupSocket_RejectInIncoming(matsSocketServer);
        setupSocket_ThrowsInIncoming(matsSocketServer);

        setupSocket_IgnoreInReplyAdapter(matsSocketServer);
        setupSocket_ResolveInReplyAdapter(matsSocketServer);
        setupSocket_RejectInReplyAdapter(matsSocketServer);
        setupSocket_ThrowsInReplyAdapter(matsSocketServer);

        setup_TestSlow(matsSocketServer, matsFactory);

        setup_ServerPush_Send(matsSocketServer, matsFactory);

        setup_ServerPush_Request_Via_Mats(matsSocketServer, matsFactory);
        setup_ServerPush_Request_Direct(matsSocketServer, matsFactory);

        setupSocket_ReplyWithCookieAuthorization(matsSocketServer);

        setupSocket_CloseThisSession(matsSocketServer);

        setupSocket_Publish(matsSocketServer);

        setupSocket_MatsSocket_renewAuth(matsSocketServer);
    }

    // ===== "Standard Endpoint".

    private static final String STANDARD_ENDPOINT = "Test.single";

    private static void setup_StandardTestSingle(MatsSocketServer matsSocketServer, MatsFactory matsFactory) {
        // :: Make default MatsSocket Endpoint
        matsSocketServer.matsSocketEndpoint(STANDARD_ENDPOINT,
                MatsSocketRequestDto.class, MatsDataTO.class, MatsSocketReplyDto.class,
                (ctx, principal, msg) -> {
                    log.info("Got MatsSocket request on MatsSocket EndpointId: " +
                            ctx.getMatsSocketEndpoint());
                    log.info(" \\- Authorization: " + ctx.getAuthorizationValue());
                    log.info(" \\- Principal:     " + ctx.getPrincipal());
                    log.info(" \\- UserId:        " + ctx.getUserId());
                    log.info(" \\- Message:       " + msg);
                    ctx.forwardCustom(new MatsDataTO(msg.number, msg.string),
                            init -> init.to(ctx.getMatsSocketEndpoint().getMatsSocketEndpointId())
                                    .interactive()
                                    .nonPersistent()
                                    .setTraceProperty("requestTimestamp", msg.requestTimestamp));
                },
                (ctx, matsReply) -> {
                    log.info("Adapting message: " + matsReply);
                    MatsSocketReplyDto reply = new MatsSocketReplyDto(matsReply.string.length(),
                            matsReply.number,
                            ctx.getMatsContext().getTraceProperty("requestTimestamp", Long.class));
                    ctx.resolve(reply);
                });

        // :: Make simple single Mats Endpoint
        matsFactory.single(STANDARD_ENDPOINT, SetupTestMatsAndMatsSocketEndpoints.MatsDataTO.class,
                SetupTestMatsAndMatsSocketEndpoints.MatsDataTO.class, (processContext, incomingDto) -> new MatsDataTO(
                        incomingDto.number,
                        incomingDto.string + ":FromSingle",
                        incomingDto.sleepTime));
    }

    private static void setup_SimpleMats(MatsSocketServer matsSocketServer, MatsFactory matsFactory) {
        // :: Make "Test.simpleMats" MatsSocket Endpoint
        matsSocketServer.matsSocketEndpoint("Test.simpleMats",
                MatsDataTO.class, MatsDataTO.class,
                (ctx, principal, msg) -> ctx.forwardInteractivePersistent(msg));

        // :: Make "Test.simpleMats" Endpoint
        matsFactory.single("Test.simpleMats", SetupTestMatsAndMatsSocketEndpoints.MatsDataTO.class,
                SetupTestMatsAndMatsSocketEndpoints.MatsDataTO.class, (processContext, incomingDto) -> new MatsDataTO(
                        incomingDto.number,
                        incomingDto.string + ":FromSimpleMats",
                        incomingDto.sleepTime));
    }

    // ===== IncomingHandler

    private static void setupSocket_IgnoreInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketDirectReplyEndpoint("Test.ignoreInIncomingHandler",
                MatsSocketRequestDto.class, MatsSocketReplyDto.class,
                // IGNORE - i.e. do nothing
                (ctx, principal, msg) -> {
                });
    }

    private static void setupSocket_DenyInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketDirectReplyEndpoint("Test.denyInIncomingHandler",
                MatsSocketRequestDto.class, MatsSocketReplyDto.class,
                // DENY
                (ctx, principal, msg) -> ctx.deny());
    }

    private static void setupSocket_ResolveInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketDirectReplyEndpoint("Test.resolveInIncomingHandler",
                MatsDataTO.class, MatsDataTO.class,
                // RESOLVE
                (ctx, principal, msg) -> ctx.resolve(
                        new MatsDataTO(msg.number, msg.string + ":From_resolveInIncomingHandler", msg.sleepTime)));
    }

    private static void setupSocket_RejectInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketDirectReplyEndpoint("Test.rejectInIncomingHandler",
                MatsSocketRequestDto.class, MatsSocketReplyDto.class,
                // REJECT
                (ctx, principal, msg) -> ctx.reject(
                        new MatsSocketReplyDto(3, 4, msg.requestTimestamp)));
    }

    private static void setupSocket_ThrowsInIncoming(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketDirectReplyEndpoint("Test.throwsInIncomingHandler",
                MatsSocketRequestDto.class, MatsSocketReplyDto.class,
                // THROW
                (ctx, principal, msg) -> {
                    throw new IllegalStateException("Exception in IncomingAuthorizationAndAdapter should REJECT");
                });
    }

    // ===== ReplyAdapter

    private static void setupSocket_IgnoreInReplyAdapter(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketEndpoint("Test.ignoreInReplyAdapter",
                MatsSocketRequestDto.class, MatsDataTO.class, MatsSocketReplyDto.class,
                (ctx, principal, msg) -> ctx.forwardCustom(new MatsDataTO(1, "string1"),
                        init -> init.to(STANDARD_ENDPOINT)),
                // IGNORE - i.e. do nothing
                (ctx, matsReply) -> {
                });
    }

    private static void setupSocket_ResolveInReplyAdapter(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketEndpoint("Test.resolveInReplyAdapter",
                MatsSocketRequestDto.class, MatsDataTO.class, MatsSocketReplyDto.class,
                (ctx, principal, msg) -> ctx.forwardCustom(new MatsDataTO(1, "string1"),
                        init -> init.to(STANDARD_ENDPOINT)),
                // RESOLVE
                (ctx, matsReply) -> ctx.resolve(new MatsSocketReplyDto(1, 2, 123)));
    }

    private static void setupSocket_RejectInReplyAdapter(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketEndpoint("Test.rejectInReplyAdapter",
                MatsSocketRequestDto.class, MatsDataTO.class, MatsSocketReplyDto.class,
                (ctx, principal, msg) -> ctx.forwardCustom(new MatsDataTO(2, "string2"),
                        init -> init.to(STANDARD_ENDPOINT)),
                // REJECT
                (ctx, matsReply) -> ctx.reject(new MatsSocketReplyDto(1, 2, 123)));
    }

    private static void setupSocket_ThrowsInReplyAdapter(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketEndpoint("Test.throwsInReplyAdapter",
                MatsSocketRequestDto.class, MatsDataTO.class, MatsSocketReplyDto.class,
                (ctx, principal, msg) -> ctx.forwardCustom(new MatsDataTO(3, "string3"),
                        init -> init.to(STANDARD_ENDPOINT)),
                // THROW
                (ctx, matsReply) -> {
                    throw new IllegalStateException("Exception in ReplyAdapter should REJECT.");
                });
    }

    // ===== Slow endpoint

    private static void setup_TestSlow(MatsSocketServer matsSocketServer, MatsFactory matsFactory) {
        // :: Forwards directly to Mats, no replyAdapter
        matsSocketServer.matsSocketEndpoint("Test.slow",
                MatsDataTO.class, MatsDataTO.class,
                (ctx, principal, msg) -> {
                    log.info("SLEEPING " + msg.sleepIncoming + " ms BEFORE FORWARDING TO MATS!");
                    try {
                        Thread.sleep(msg.sleepIncoming);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    ctx.forwardInteractivePersistent(msg);
                });

        // :: Simple endpoint that just sleeps a tad, to simulate "long(er) running process".
        matsFactory.single("Test.slow", MatsDataTO.class, MatsDataTO.class,
                (processContext, incomingDto) -> {
                    if (incomingDto.sleepTime > 0) {
                        log.info("incoming.sleepTime > 0, sleeping specified [" + incomingDto.sleepTime + "] ms.");
                        try {
                            Thread.sleep(incomingDto.sleepTime);
                        }
                        catch (InterruptedException e) {
                            throw new AssertionError("Got interrupted while slow-sleeping..!");
                        }
                    }
                    return new MatsDataTO(incomingDto.number,
                            incomingDto.string + ":FromSlow",
                            incomingDto.sleepTime);
                });
    }

    // ===== Server Push: MatsSocketServer.send(..) and .request(..)

    private static void setup_ServerPush_Send(MatsSocketServer matsSocketServer, MatsFactory matsFactory) {
        matsSocketServer.matsSocketTerminator("Test.server.send.matsStage",
                MatsDataTO.class, (ctx, principal, msg) -> ctx.forwardCustom(new MatsDataTO(msg.number,
                        ctx.getMatsSocketSessionId(), 1), init -> init.to("Test.server.send")));

        matsSocketServer.matsSocketTerminator("Test.server.send.thread",
                MatsDataTO.class, (ctx, principal, msg) -> ctx.forwardCustom(new MatsDataTO(msg.number,
                        ctx.getMatsSocketSessionId(), 2), init -> init.to("Test.server.send")));

        // :: Simple endpoint that does a MatsSocketServer.send(..), either inside MatsStage, or in separate thread.
        matsFactory.terminator("Test.server.send", Void.class, MatsDataTO.class,
                (processContext, state, incomingDto) -> {
                    if (incomingDto.sleepTime == 1) {
                        // Fire the sending off directly within the MatsStage, to prove that this is possible.
                        matsSocketServer.send(incomingDto.string, processContext.getTraceId()
                                + ":SentFromMatsStage", "ClientSide.terminator", incomingDto);
                    }
                    else if (incomingDto.sleepTime == 2) {
                        // Fire the sending in a new Thread, to prove that this can be done totally outside context.
                        new Thread(() -> {
                            matsSocketServer.send(incomingDto.string, processContext.getTraceId()
                                    + ":SentFromThread", "ClientSide.terminator", incomingDto);
                        }, "Test-MatsSocketServer.send()").start();
                    }
                });

    }

    private static void setup_ServerPush_Request_Direct(MatsSocketServer matsSocketServer, MatsFactory matsFactory) {
        // Receives the "start", which starts the cascade - and performs a Server-to-Client Request to Client Endpoint,
        // setting the replyTo to the following MatsSocket endpoint.
        matsSocketServer.matsSocketTerminator("Test.server.request.direct",
                MatsDataTO.class, (ctx, principal, msg) -> {
                    // Send request Server-to-Client, passing the message directly on.
                    // NOTE: Client side will add a bit to it!
                    matsSocketServer.request(ctx.getMatsSocketSessionId(), ctx.getTraceId(),
                            "ClientSide.endpoint", msg,
                            "Test.server.request.replyReceiver.direct",
                            "CorrelationString", "CorrelationBinary".getBytes(StandardCharsets.UTF_8));
                });

        // .. which gets the Reply from Client, and forwards it to the following Mats endpoint
        matsSocketServer.matsSocketTerminator("Test.server.request.replyReceiver.direct",
                MatsDataTO.class, (ctx, principal, msg) -> {
                    // Assert the Correlation information
                    Assert.assertEquals("CorrelationString", ctx.getCorrelationString());
                    Assert.assertArrayEquals("CorrelationBinary".getBytes(StandardCharsets.UTF_8),
                            ctx.getCorrelationBinary());
                    // Send to Client, add whether it was a RESOLVE or REJECT in the message.
                    matsSocketServer.send(ctx.getMatsSocketSessionId(), ctx.getTraceId(),
                            "ClientSide.terminator",
                            new MatsDataTO(msg.number, msg.string + ':' + ctx.getMessageType().name(),
                                    msg.sleepTime));
                });
    }

    private static void setup_ServerPush_Request_Via_Mats(MatsSocketServer matsSocketServer, MatsFactory matsFactory) {
        // Receives the "start", which starts the cascade - and forwards to the following Mats endpoint
        matsSocketServer.matsSocketTerminator("Test.server.request.viaMats",
                MatsDataTO.class,
                // Pass the message directly on
                (ctx, principal, msg) -> ctx.forwardCustom(msg,
                        init -> init.to("Test.server.requestToClient.viaMats")));

        // .. which initiates a request to Client Endpoint, asking for Reply to go to the following MatsSocket Endpoint
        matsFactory.terminator("Test.server.requestToClient.viaMats", Void.class, MatsDataTO.class,
                (processContext, state, msg) -> {
                    String matsSocketSessionId = processContext.getString("matsSocketSessionId");
                    matsSocketServer.request(matsSocketSessionId, processContext.getTraceId(),
                            "ClientSide.endpoint",
                            msg, // Pass the message directly on. NOTE: Client side will add a bit to it!
                            "Test.server.request.replyReceiver.viaMats",
                            "CorrelationString", "CorrelationBinary".getBytes(StandardCharsets.UTF_8));
                });

        // .. which gets the Reply from Client, and forwards it to the following Mats endpoint
        matsSocketServer.matsSocketTerminator("Test.server.request.replyReceiver.viaMats",
                MatsDataTO.class, (ctx, principal, msg) -> {
                    // Assert the Correlation information
                    Assert.assertEquals("CorrelationString", ctx.getCorrelationString());
                    Assert.assertArrayEquals("CorrelationBinary".getBytes(StandardCharsets.UTF_8),
                            ctx.getCorrelationBinary());
                    // Forward to the Mats terminator that sends to Client. Pass message directly on.
                    ctx.forwardCustom(msg, init -> {
                        init.addString("resolveReject", ctx.getMessageType().name());
                        init.to("Test.server.sendReplyBackToClient.viaMats");
                    });
                });

        // .. which finally sends the Reply back to the Client Terminator.
        matsFactory.terminator("Test.server.sendReplyBackToClient.viaMats", Void.class, MatsDataTO.class,
                (ctx, state, msg) -> {
                    String matsSocketSessionId = ctx.getString("matsSocketSessionId");
                    // Send to Client, add whether it was a RESOLVE or REJECT in the message.
                    matsSocketServer.send(matsSocketSessionId, ctx.getTraceId(),
                            "ClientSide.terminator",
                            new MatsDataTO(msg.number, msg.string + ':' + ctx.getString("resolveReject"),
                                    msg.sleepTime));
                });
    }

    private static void setupSocket_ReplyWithCookieAuthorization(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketDirectReplyEndpoint("Test.replyWithCookieAuthorization",
                MatsDataTO.class, MatsDataTO.class,
                // RESOLVE
                (ctx, principal, msg) -> ctx.resolve(
                        new MatsDataTO(msg.number, ((DummyAuthPrincipal) principal).getAuthorizationHeaderFromCookie(),
                                msg.sleepTime)));
    }

    private static void setupSocket_CloseThisSession(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketTerminator("Test.closeThisSession",
                MatsDataTO.class,
                // Perform 'server.closeSession(thisSession)' in a new Thread.
                (ctx, principal, msg) -> {
                    new Thread(() -> {
                        try {
                            Thread.sleep(100);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        matsSocketServer.closeSession(ctx.getMatsSocketSessionId());
                    }, "Mats CloseThisSession").start();
                });
    }

    private static void setupSocket_Publish(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketTerminator("Test.publish",
                String.class, (ctx, principal, msg) -> {
                    new Thread(() -> {
                        matsSocketServer.publish("Test 1 2 3", "Test.topic", new MatsDataTO(Math.PI, "Test from Java!",
                                42));
                    }, "Send message").start();
                });
    }

    private static void setupSocket_MatsSocket_renewAuth(MatsSocketServer matsSocketServer) {
        matsSocketServer.matsSocketTerminator("Test.renewAuth",
                MatsDataTO.class,
                (ctx, principal, msg) -> matsSocketServer.request(ctx.getMatsSocketSessionId(), ctx.getTraceId(),
                        "MatsSocket.renewAuth", "", "Test.renewAuth_reply", "123", new byte[] { 1, 2, 3 }));

        matsSocketServer.matsSocketTerminator("Test.renewAuth_reply",
                MatsDataTO.class, (ctx, principal, msg) -> {
                    Assert.assertEquals("123", ctx.getCorrelationString());
                    Assert.assertArrayEquals(new byte[] { 1, 2, 3 }, ctx.getCorrelationBinary());

                    matsSocketServer.send(ctx.getMatsSocketSessionId(), ctx.getTraceId(),
                            "Client.renewAuth_terminator", ctx.getAuthorizationValue());
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
                throw new AssertionError(MatsSocketRequestDto.class.getSimpleName() + " was attempted equalled to ["
                        + obj + "].");
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
        public int sleepTime;
        public int sleepIncoming;

        public MatsDataTO() {
        }

        public MatsDataTO(double number, String string) {
            this.number = number;
            this.string = string;
        }

        public MatsDataTO(double number, String string, int sleepTime) {
            this.number = number;
            this.string = string;
            this.sleepTime = sleepTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            // NOTICE: Not Class-equals, but "instanceof", since we accept the "SubDataTO" too.
            if (o == null || !(o instanceof MatsDataTO)) return false;
            MatsDataTO matsDataTO = (MatsDataTO) o;
            return Double.compare(matsDataTO.number, number) == 0 &&
                    sleepTime == matsDataTO.sleepTime &&
                    Objects.equals(string, matsDataTO.string);
        }

        @Override
        public int hashCode() {
            return Objects.hash(number, string, sleepTime);
        }

        @Override
        public String toString() {
            return "MatsDataTO [number=" + number
                    + ", string=" + string
                    + (sleepTime != 0 ? ", multiplier=" + sleepTime : "")
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
                throw new AssertionError(MatsSocketReplyDto.class.getSimpleName() + " was attempted equalled to [" + obj
                        + "].");
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
