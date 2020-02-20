package com.stolsvik.mats.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpoint;

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
    }

    private static void setupStandardTestSingle(MatsSocketServer matsSocketServer) {
        // :: Make default MatsSocket Endpoint
        MatsSocketEndpoint<MatsSocketRequestDto, MatsDataTO, MatsDataTO, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint("Test.single",
                        MatsSocketRequestDto.class, MatsDataTO.class, MatsDataTO.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> {
                            log.info("Got MatsSocket request on MatsSocket EndpointId: " +
                                    ctx.getMatsSocketEndpointId());
                            log.info(" \\- Authorization: " + ctx.getAuthorizationHeader());
                            log.info(" \\- Principal:     " + ctx.getPrincipal());
                            log.info(" \\- UserId:        " + ctx.getUserId());
                            log.info(" \\- Message:       " + msIncoming);
                            ctx.forwardCustom(new MatsDataTO(msIncoming.number, msIncoming.string),
                                    msg -> {
                                        msg.to(ctx.getMatsSocketEndpointId())
                                                .interactive()
                                                .nonPersistent()
                                                .setTraceProperty("requestTimestamp", msIncoming.requestTimestamp);
                                    });
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
        MatsSocketEndpoint<MatsSocketRequestDto, Void, Void, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint("Test.resolveInIncomingHandler",
                        MatsSocketRequestDto.class, Void.class, Void.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> ctx.resolve(
                                new MatsSocketReplyDto(1, 2, msIncoming.requestTimestamp)));
    }

    private static void setupRejectInIncoming(MatsSocketServer matsSocketServer) {
        MatsSocketEndpoint<MatsSocketRequestDto, Void, Void, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint("Test.rejectInIncomingHandler",
                        MatsSocketRequestDto.class, Void.class, Void.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> ctx.reject(
                                new MatsSocketReplyDto(3, 4, msIncoming.requestTimestamp)));
    }

    private static void setupThrowsInIncoming(MatsSocketServer matsSocketServer) {
        MatsSocketEndpoint<MatsSocketRequestDto, Void, Void, MatsSocketReplyDto> matsSocketEndpoint = matsSocketServer
                .matsSocketEndpoint("Test.throwsInIncomingHandler",
                        MatsSocketRequestDto.class, Void.class, Void.class, MatsSocketReplyDto.class,
                        (ctx, principal, msIncoming) -> {
                            throw new IllegalStateException("Should reject.");
                        });
    }

}
