package com.stolsvik.mats.lib_test.intercept;

import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;

import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.MatsInitiator.MatsInitiateWrapper;
import com.stolsvik.mats.MatsInitiator.MessageReference;
import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor;
import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor.MatsInitiateInterceptInterceptor;
import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor.MatsInitiateMessageInterceptor;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.test.MatsTestHelp;
import com.stolsvik.mats.test.junit.Rule_Mats;

/**
 * Simple interception test which doesn't really test anything except showing, in the logs, the code sequence described
 * in JavaDoc of {@link MatsInitiateInterceptor}.
 *
 * @author Endre Stølsvik - 2021-01-17 13:35 - http://endre.stolsvik.com
 */
public class Test_InitiationIntercept_JavaDoc {
    private static final Logger log = MatsTestHelp.getClassLogger();

    @ClassRule
    public static final Rule_Mats MATS = Rule_Mats.create();

    @Test
    public void doTest() {
        final MatsInitiateInterceptor initiationInterceptor_1 = new MyMatsInitiateInterceptor(1);
        final MatsInitiateInterceptor initiationInterceptor_2 = new MyMatsInitiateInterceptor(2);

        MATS.getJmsMatsFactory().addInitiationInterceptorSingleton(initiationInterceptor_1);
        MATS.getJmsMatsFactory().addInitiationInterceptorSingleton(initiationInterceptor_2);

        MATS.getMatsFactory().getDefaultInitiator().initiateUnchecked(init -> {
            init.traceId(MatsTestHelp.traceId() + "_First")
                    .from(MatsTestHelp.from("test"))
                    .to(MatsTestHelp.terminator())
                    .send(new DataTO(1, "First message"));
            init.traceId(MatsTestHelp.traceId() + "_Second")
                    .from(MatsTestHelp.from("test"))
                    .to(MatsTestHelp.terminator())
                    .send(new DataTO(2, "Second message"));
        });
    }

    private static class MyMatsInitiateInterceptor implements MatsInitiateInterceptor,
            MatsInitiateInterceptInterceptor, MatsInitiateMessageInterceptor {

        private final int _number;

        MyMatsInitiateInterceptor(int number) {
            _number = number;
        }

        @Override
        public void initiateStarted(InitiateStartedContext initiateStartedContext) {
            log.info("Stage 'Started', interceptor #" + _number);
        }

        @Override
        public void interceptInitiateUserLambda(InitiateInterceptUserLambdaContext initiateInterceptUserLambdaContext,
                InitiateLambda initiateLambda, MatsInitiate matsInitiate) {
            log.info("Stage 'Intercept', pre lambda-invoke, interceptor #" + _number);

            // Example: Wrap the MatsInitiate to catch "send(dto)", before invoking the lambda
            MatsInitiateWrapper wrappedMatsInitiate = new MatsInitiateWrapper(matsInitiate) {
                @Override
                public MessageReference send(Object messageDto) {
                    log.info(".. matsInitiate.send(..), pre super.send(..), interceptor #" + _number + ", message:"
                            + messageDto);
                    MessageReference sendMsgRef = super.send(messageDto);
                    log.info(".. matsInitiate.send(..), post super.send(..), interceptor #" + _number + ", message:"
                            + messageDto
                            + ", messageReference:" + sendMsgRef);
                    return sendMsgRef;
                }
            };

            // Invoke the lambda, with the wrapped MatsInitiate
            initiateLambda.initiate(wrappedMatsInitiate);

            // NOTICE: At this point the MDC's "traceId" will be whatever the last initiated message set it to.

            log.info("Stage 'Intercept', post lambda-invoke, interceptor #" + _number);
        }

        @Override
        public void interceptInitiateOutgoingMessages(
                InitiateInterceptOutgoingMessagesContext initiateInterceptOutgoingMessagesContext) {
            log.info("Stage 'Message', interceptor #" + _number + ", message:" + initiateInterceptOutgoingMessagesContext
                    .getOutgoingMessage().getMessage());
        }

        @Override
        public void initiateCompleted(InitiateCompletedContext initiateCompletedContext) {
            log.info("Stage 'Completed', interceptor #" + _number + ", messages:" + initiateCompletedContext
                    .getOutgoingMessages());
        }

    }
}
