package com.stolsvik.mats.websocket.impl;

/**
 * @author Endre Stølsvik 2020-01-15 08:38 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsSocketStatics {

    String MDC_SESSION_ID = "matssocket.sessionId";
    String MDC_PRINCIPAL_NAME = "matssocket.principal";
    String MDC_USER_ID = "matssocket.userId";

    String MDC_CLIENT_LIB_AND_VERSIONS = "matssocket.clv";
    String MDC_CLIENT_APP_NAME_AND_VERSION = "matssocket.clientApp";

    String MDC_MESSAGE_TYPE = "matssocket.msgType";
    String MDC_TRACE_ID = "traceId";

    default float ms(long nanos) {
        return (float) (Math.round(nanos / 10_000d) / 1_00d);
    }

    default float msSince(long nanosStart) {
        return ms(System.nanoTime() - nanosStart);
    }

    class DebugStackTrace extends Exception {
        public DebugStackTrace(String what) {
            super("Debug Stacktrace to record where " + what + " happened.");
        }
    }
}
