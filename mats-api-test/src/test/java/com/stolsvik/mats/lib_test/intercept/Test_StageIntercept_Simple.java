package com.stolsvik.mats.lib_test.intercept;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsEndpoint.ProcessContextWrapper;
import com.stolsvik.mats.MatsEndpoint.ProcessLambda;
import com.stolsvik.mats.MatsInitiator.MessageReference;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.MatsStageInterceptInterceptor;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.MatsStageMessageInterceptor;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestHelp;
import com.stolsvik.mats.test.junit.Rule_Mats;

/**
 * Simple test adding a MatsStageInterceptor, just for visual inspection in logs.
 */
public class Test_StageIntercept_Simple {
    private static final Logger log = MatsTestHelp.getClassLogger();

    @ClassRule
    public static final Rule_Mats MATS = Rule_Mats.create();

    private static final String SERVICE = MatsTestHelp.service();
    private static final String TERMINATOR = MatsTestHelp.terminator();

    private static final CountDownLatch _latch = new CountDownLatch(2);

    @BeforeClass
    public static void setupService() {
        MATS.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> new DataTO(dto.number * 2, dto.string + ":FromService"));
    }

    @BeforeClass
    public static void setupTerminator() {
        MATS.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    _latch.countDown();
                });

    }

    @Test
    public void doTest() throws InterruptedException {
        final MatsStageInterceptor stageInterceptor_1 = new MyMatsStageInterceptor(1);
        final MatsStageInterceptor stageInterceptor_2 = new MyMatsStageInterceptor(2);

        MATS.getJmsMatsFactory().addStageInterceptorSingleton(stageInterceptor_1);
        MATS.getJmsMatsFactory().addStageInterceptorSingleton(stageInterceptor_2);

        MATS.getMatsFactory().getDefaultInitiator().initiateUnchecked(init -> {
            init.traceId(MatsTestHelp.traceId() + "_First")
                    .from(MatsTestHelp.from("test"))
                    .to(SERVICE)
                    .replyTo(TERMINATOR, new DataTO(0, "null"))
                    .request(new DataTO(1, "First message"));
            init.traceId(MatsTestHelp.traceId() + "_Second")
                    .from(MatsTestHelp.from("test"))
                    .to(SERVICE)
                    .replyTo(TERMINATOR, new DataTO(0, "null"))
                    .request(new DataTO(2, "Second message"));
        });

        _latch.await(5, TimeUnit.SECONDS);
    }

    private static class MyMatsStageInterceptor implements MatsStageInterceptor,
            MatsStageInterceptInterceptor, MatsStageMessageInterceptor {

        private final int _number;

        MyMatsStageInterceptor(int number) {
            _number = number;
        }

        @Override
        public void receiveDeconstructError(StageReceiveDeconstructErrorContext stageReceiveDeconstructErrorContext) {

        }

        @Override
        public void stageReceived(StageReceivedContext stageReceivedContext) {
            log.info("Stage 'Started', interceptor #" + _number);
        }

        @Override
        public void interceptStageUserLambda(StageInterceptUserLambdaContext processingIntercept,
                ProcessLambda<Object, Object, Object> processLambda,
                ProcessContext<Object> ctx, Object state, Object msg) throws MatsRefuseMessageException {
            log.info("Stage 'Intercept', pre lambda-invoke, interceptor #" + _number);

            // Example: Wrap the ProcessContext to catch "reply(dto)", before invoking the lambda
            ProcessContextWrapper<Object> wrappedProcessContext = new ProcessContextWrapper<Object>(ctx) {
                @Override
                public MessageReference reply(Object replyDto) {
                    log.info(".. processContext.reply(..), pre super.reply(..), interceptor #" + _number + ", message:"
                            + replyDto);
                    MessageReference sendMsgRef = super.reply(replyDto);
                    log.info(".. processContext.reply(..), post super.reply(..), interceptor #" + _number + ", message:"
                            + replyDto + ", messageReference:" + sendMsgRef);
                    return sendMsgRef;
                }
            };

            processLambda.process(wrappedProcessContext, state, msg);

            log.info("Stage 'Intercept', post lambda-invoke, interceptor #" + _number);
        }

        @Override
        public void interceptStageOutgoingMessages(
                StageInterceptOutgoingMessageContext stageInterceptOutgoingMessageContext) {
            log.info("Stage 'Message', interceptor #" + _number + ", message:" + stageInterceptOutgoingMessageContext
                    .getOutgoingMessage().getMessage());
        }

        @Override
        public void stageCompleted(StageCompletedContext stageCompletedContext) {
            log.info("Stage 'Completed', interceptor #" + _number + ", messages:" + stageCompletedContext
                    .getOutgoingMessages());
        }
    }

}
