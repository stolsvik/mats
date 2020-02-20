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

}
