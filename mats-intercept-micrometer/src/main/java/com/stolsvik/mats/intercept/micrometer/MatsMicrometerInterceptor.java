package com.stolsvik.mats.intercept.micrometer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.api.intercept.CommonCompletedContext;
import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor;
import com.stolsvik.mats.api.intercept.MatsInterceptable;
import com.stolsvik.mats.api.intercept.MatsMetricsInterceptor;
import com.stolsvik.mats.api.intercept.MatsOutgoingMessage.MatsSentOutgoingMessage;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

/**
 * An interceptor that instruments a MatsFactory with metrics using the (Spring) Micrometer framework. If you provide a
 * {@link MeterRegistry}, it will employ this to create the metrics on, otherwise it employs the
 * {@link Metrics#globalRegistry}.
 * <p />
 * Simple way to add it: {@link #install(MatsInterceptable, MeterRegistry)} or {@link #install(MatsInterceptable)},
 * which returns the installed interceptor instance (so you could potentially can remove it again).
 *
 * @author Endre Stølsvik - 2021-02-07 12:45 - http://endre.stolsvik.com
 */
public class MatsMicrometerInterceptor
        implements MatsMetricsInterceptor, MatsInitiateInterceptor, MatsStageInterceptor {

    public static final Logger log_init = LoggerFactory.getLogger("com.stolsvik.mats.metrics.init");
    public static final Logger log_stage = LoggerFactory.getLogger("com.stolsvik.mats.metrics.stage");

    public static final String LOG_PREFIX = "#MATSMETRICS# ";

    private final MeterRegistry _meterRegistry;

    private MatsMicrometerInterceptor(MeterRegistry meterRegistry) {
        _meterRegistry = meterRegistry;
    }

    /**
     * Creates a {@link MatsMicrometerInterceptor} employing the provided {@link MeterRegistry}, and installs it as a
     * singleton on the provided {@link MatsInterceptable} (which most probably is a {@link MatsFactory}).
     * 
     * @param matsInterceptableMatsFactory
     *            the {@link MatsInterceptable} to install on (probably a {@link MatsFactory}.
     * @param meterRegistry
     *            the Micrometer {@link MeterRegistry} to create meters on.
     * @return the {@link MatsMicrometerInterceptor} instance which was installed as singleton.
     */
    public static MatsMicrometerInterceptor install(
            MatsInterceptable matsInterceptableMatsFactory,
            MeterRegistry meterRegistry) {
        MatsMicrometerInterceptor metrics = new MatsMicrometerInterceptor(meterRegistry);
        matsInterceptableMatsFactory.addInitiationInterceptorSingleton(metrics);
        matsInterceptableMatsFactory.addStageInterceptorSingleton(metrics);
        return metrics;
    }

    private static final MatsMicrometerInterceptor GLOBAL_REGISTRY_INSTANCE = new MatsMicrometerInterceptor(
            Metrics.globalRegistry);

    /**
     * Installs a singleton instance of {@link MatsMicrometerInterceptor} which employs the
     * {@link Metrics#globalRegistry}, on the provided {@link MatsInterceptable} (which most probably is a
     * {@link MatsFactory}).
     *
     * @param matsInterceptable
     *            the {@link MatsInterceptable} to install on (probably a {@link MatsFactory}.
     * @return the {@link MatsMicrometerInterceptor} instance which was installed as singleton.
     */
    public static MatsMicrometerInterceptor install(MatsInterceptable matsInterceptable) {
        matsInterceptable.addInitiationInterceptorSingleton(GLOBAL_REGISTRY_INSTANCE);
        matsInterceptable.addStageInterceptorSingleton(GLOBAL_REGISTRY_INSTANCE);
        return GLOBAL_REGISTRY_INSTANCE;
    }

    @Override
    public void initiateCompleted(InitiateCompletedContext ctx) {
        List<MatsSentOutgoingMessage> outgoingMessages = ctx.getOutgoingMessages();

        if (!outgoingMessages.isEmpty()) {
            MatsSentOutgoingMessage firstMessage = outgoingMessages.get(0);
            String fromId = firstMessage.getFrom();
            Timer timer_TotalTime = Timer.builder("mats.init.total")
                    .tag("initiator", ctx.getMatsInitiator().getName())
                    .tag("from", fromId)
                    .description("Total time taken to execute initialization")
                    .register(_meterRegistry);
            timer_TotalTime.record(ctx.getTotalExecutionNanos(), TimeUnit.NANOSECONDS);

            Timer timer_DbCommit = Timer.builder("mats.init.dbcommit")
                    .tag("initiator", ctx.getMatsInitiator().getName())
                    .tag("from", fromId)
                    .description("Time taken to commit database")
                    .register(_meterRegistry);
            timer_DbCommit.record(ctx.getDbCommitNanos(), TimeUnit.NANOSECONDS);

            Timer timer_MsgSend = Timer.builder("mats.init.msgsend")
                    .tag("initiator", ctx.getMatsInitiator().getName())
                    .tag("from", fromId)
                    .description("Time taken (sum) to produce and send messages to message system")
                    .register(_meterRegistry);
            timer_MsgSend.record(ctx.getSumMessageSystemProductionAndSendNanos(), TimeUnit.NANOSECONDS);

            Timer timer_MsgCommit = Timer.builder("mats.init.msgcommit")
                    .tag("initiator", ctx.getMatsInitiator().getName())
                    .tag("from", fromId)
                    .description("Time taken to commit message system")
                    .register(_meterRegistry);
            timer_MsgCommit.record(ctx.getMessageSystemCommitNanos(), TimeUnit.NANOSECONDS);


            for (MatsSentOutgoingMessage msg : outgoingMessages) {
                DistributionSummary size = DistributionSummary.builder("mats.msg.out")
                        .tag("from", msg.getFrom())
                        .tag("to", msg.getTo())
                        .baseUnit("bytes")
                        .description("Outgoing mats message wire size")
                        .register(_meterRegistry);
                size.record(msg.getEnvelopeWireSize());
            }
        }

        String messageSenderName = ctx.getMatsInitiator().getParentFactory()
                .getFactoryConfig().getName() + "|" + ctx.getMatsInitiator().getName();

        completed(ctx, "", log_init, outgoingMessages, messageSenderName, "", 0L);
    }

    @Override
    public void stageReceived(StageReceivedContext ctx) {
        ProcessContext<Object> processContext = ctx.getProcessContext();

        long sumNanosPieces = ctx.getMessageSystemDeconstructNanos()
                + ctx.getEnvelopeDecompressionNanos()
                + ctx.getEnvelopeDeserializationNanos()
                + ctx.getMessageAndStateDeserializationNanos();

        log_stage.info(LOG_PREFIX + "RECEIVED message from [" + processContext.getFromStageId()
                + "@" + processContext.getFromAppName() + ",v." + processContext.getFromAppVersion()
                + "], totPreprocAndDeserial:[" + ms(ctx.getTotalPreprocessAndDeserializeNanos())
                + "] || breakdown: msgSysDeconstruct:[" + ms(ctx.getMessageSystemDeconstructNanos())
                + " ms]->envelopeWireSize:[" + ctx.getEnvelopeWireSize()
                + " B]->decomp:[" + ms(ctx.getEnvelopeDecompressionNanos())
                + " ms]->serialSize:[" + ctx.getEnvelopeSerializedSize()
                + " B]->deserial:[" + ms(ctx.getEnvelopeDeserializationNanos())
                + " ms]->(envelope)->dto&stoDeserial:[" + ms(ctx.getMessageAndStateDeserializationNanos())
                + " ms] - sum pieces:[" + ms(sumNanosPieces)
                + " ms], diff:[" + ms(ctx.getTotalPreprocessAndDeserializeNanos() - sumNanosPieces)
                + " ms]");
    }

    @Override
    public void stageCompleted(StageCompletedContext ctx) {

        List<MatsSentOutgoingMessage> outgoingMessages = ctx.getOutgoingMessages();

        Timer timer_TotalTime = Timer.builder("mats.stage")
                .tag("stageId", ctx.getProcessContext().getStageId())
                .description("Total time taken to execute stage")
                .register(_meterRegistry);
        timer_TotalTime.record(ctx.getTotalExecutionNanos(), TimeUnit.NANOSECONDS);


        for (MatsSentOutgoingMessage msg : outgoingMessages) {
            DistributionSummary distSum_size = DistributionSummary.builder("mats.msg.size")
                    .tag("from", msg.getFrom())
                    .tag("to", msg.getTo())
                    .baseUnit("bytes")
                    .description("Outgoing mats message wire size")
                    .register(_meterRegistry);
            distSum_size.record(msg.getEnvelopeWireSize());

            Timer timer_MsgTime = Timer.builder("mats.msg.time")
                    .tag("from", msg.getFrom())
                    .tag("to", msg.getTo())
                    .description("Total time taken to create, serialize and send message")
                    .register(_meterRegistry);
            timer_MsgTime.record(msg.getEnvelopeProduceNanos()
                    + msg.getEnvelopeSerializationNanos()
                    + msg.getEnvelopeCompressionNanos()
                    + msg.getMessageSystemProductionAndSendNanos(), TimeUnit.NANOSECONDS);
        }



        String messageSenderName = ctx.getStage().getParentEndpoint().getParentFactory()
                .getFactoryConfig().getName();
        // :: Specific metric for stage completed
        String extraBreakdown = " totPreprocAndDeserial:[" + ms(ctx.getTotalPreprocessAndDeserializeNanos())
                + " ms],";
        long extraNanosBreakdown = ctx.getTotalPreprocessAndDeserializeNanos();
        completed(ctx, " with result " + ctx.getProcessResult(), log_stage,
                outgoingMessages, messageSenderName, extraBreakdown, extraNanosBreakdown);
    }

    private void completed(CommonCompletedContext ctx, String extraResult, Logger logger,
            List<MatsSentOutgoingMessage> outgoingMessages, String messageSenderName,
            String extraBreakdown, long extraNanosBreakdown) {

        String what = extraBreakdown.isEmpty() ? "INIT" : "STAGE";

        // ?: Do we have a Throwable, indicating that processing (stage or init) failed?
        if (ctx.getThrowable().isPresent()) {
            // -> Yes, we have a Throwable
            // :: Output the 'completed' (now FAILED) line
            // NOTE: We do NOT output any messages, even if there are any (they can have been produced before the
            // error occurred), as they will NOT have been sent, and the log lines are supposed to represent
            // actual messages that have been put on the wire.
            Throwable t = ctx.getThrowable().get();
            completedLog(ctx, LOG_PREFIX + what + " !!FAILED!!" + extraResult, extraBreakdown, extraNanosBreakdown,
                    Collections.emptyList(), Level.ERROR, logger, t, "");
        }
        else if (outgoingMessages.size() != 1) {
            // -> Yes, >1 or 0 messages

            // :: Output the 'completed' line
            completedLog(ctx, LOG_PREFIX + what + " completed" + extraResult, extraBreakdown, extraNanosBreakdown,
                    outgoingMessages, Level.INFO, logger, null, "");

            // :: Output the per-message logline (there might be >1, or 0, messages)
            for (MatsSentOutgoingMessage outgoingMessage : ctx.getOutgoingMessages()) {
                msgMdcLog(outgoingMessage, () -> log_init.info(LOG_PREFIX
                        + msgLogLine(messageSenderName, outgoingMessage)));
            }
        }
        else {
            // -> Only 1 message and no Throwable: Concat the two lines.
            msgMdcLog(outgoingMessages.get(0), () -> {
                String msgLine = msgLogLine(messageSenderName, outgoingMessages.get(0));
                completedLog(ctx, LOG_PREFIX + what + " completed" + extraResult, extraBreakdown, extraNanosBreakdown,
                        outgoingMessages, Level.INFO, logger, null, "\n    " + LOG_PREFIX + msgLine);
            });
        }
    }

    private void msgMdcLog(MatsSentOutgoingMessage msg, Runnable runnable) {
        // :: Actually run the Runnable
        runnable.run();
    }

    private String msgLogLine(String messageSenderName, MatsSentOutgoingMessage msg) {
        long nanosTaken_Total = msg.getEnvelopeProduceNanos()
                + msg.getEnvelopeSerializationNanos()
                + msg.getEnvelopeCompressionNanos()
                + msg.getMessageSystemProductionAndSendNanos();

        return msg.getDispatchType() + " outgoing " + msg.getMessageType()
                + " message from [" + messageSenderName + "|" + msg.getFrom()
                + "] -> [" + msg.getTo()
                + "], total:[" + ms(nanosTaken_Total)
                + " ms] || breakdown: produce:[" + ms(msg.getEnvelopeProduceNanos())
                + " ms]->(envelope)->serial:[" + ms(msg.getEnvelopeSerializationNanos())
                + " ms]->serialSize:[" + msg.getEnvelopeSerializedSize()
                + " B]->comp:[" + ms(msg.getEnvelopeCompressionNanos())
                + " ms]->envelopeWireSize:[" + msg.getEnvelopeWireSize()
                + " B]->msgSysConstruct&Send:[" + ms(msg.getMessageSystemProductionAndSendNanos())
                + " ms]";
    }

    private enum Level {
        INFO, ERROR
    }

    private void completedLog(CommonCompletedContext ctx, String logPrefix, String extraBreakdown,
            long extraNanosBreakdown, List<MatsSentOutgoingMessage> outgoingMessages, Level level, Logger log,
            Throwable t, String logPostfix) {

        /*
         * NOTE! The timings are a bit user-unfriendly wrt. "user lambda" timing, as this includes the production of
         * outgoing envelopes, including DTO, STO and TraceProps serialization, due to how it must be implemented. Thus,
         * we do some tricking here to get more relevant "split up" of the separate pieces.
         */
        // :: Find total DtoAndSto Serialization of outgoing messages
        long nanosTaken_SumDtoAndStoSerialNanos = 0;
        for (MatsSentOutgoingMessage msg : outgoingMessages) {
            nanosTaken_SumDtoAndStoSerialNanos += msg.getEnvelopeProduceNanos();
        }

        // :: Subtract the total DtoAndSto Serialization from the user lambda time.
        long nanosTaken_UserLambdaAlone = ctx.getUserLambdaNanos() - nanosTaken_SumDtoAndStoSerialNanos;

        // :: Sum up the total "message handling": Dto&Sto + envelope serial + msg.sys. handling.
        long nanosTaken_SumMessageOutHandling = nanosTaken_SumDtoAndStoSerialNanos
                + ctx.getSumEnvelopeSerializationAndCompressionNanos()
                + ctx.getSumMessageSystemProductionAndSendNanos();

        // Sum of all the pieces, to compare against the total execution time.
        long nanosTaken_SumPieces = extraNanosBreakdown
                + nanosTaken_UserLambdaAlone
                + nanosTaken_SumMessageOutHandling
                + ctx.getDbCommitNanos()
                + ctx.getMessageSystemCommitNanos();

        String numMessages;
        switch (outgoingMessages.size()) {
            case 0:
                numMessages = "no outgoing messages";
                break;
            case 1:
                numMessages = "single outgoing " + outgoingMessages.get(0).getMessageType() + " message";
                break;
            default:
                numMessages = "[" + outgoingMessages.size() + "] outgoing messages";
        }

        String msg = logPrefix
                + ", " + numMessages
                + ", total:[" + ms(ctx.getTotalExecutionNanos()) + " ms]"
                + " || breakdown:"
                + extraBreakdown
                + " userLambda (excl. produceEnvelopes):[" + ms(nanosTaken_UserLambdaAlone)
                + " ms], sumMsgOutHandling:[" + ms(nanosTaken_SumMessageOutHandling)
                + " ms], dbCommit:[" + ms(ctx.getDbCommitNanos())
                + " ms], msgSysCommit:[" + ms(ctx.getMessageSystemCommitNanos())
                + " ms] - sum pieces:[" + ms(nanosTaken_SumPieces)
                + " ms], diff:[" + ms(ctx.getTotalExecutionNanos() - nanosTaken_SumPieces)
                + " ms]"
                + logPostfix;

        // ?: Log at what level?
        if (level == Level.INFO) {
            // -> INFO
            log.info(msg);
        }
        else {
            // -> ERROR
            log.error(msg, t);
        }
    }

    private static String msS(long nanosTake) {
        return Double.toString(ms(nanosTake));
    }

    /**
     * Converts nanos to millis with a sane number of decimals ("3.5" significant digits). Takes care of handling the
     * difference between 0 and >0 nanoseconds, wrt. the rounding - in that 1 nanosecond will become (a tad wrong)
     * 0.0001, while 0 will be 0.0.
     */
    private static double ms(long nanosTaken) {
        if (nanosTaken == 0) {
            return 0.0;
        }
        // >=500 ms?
        if (nanosTaken >= 1_000_000 * 500) {
            // -> Yes, >500ms, so drop decimals entirely
            return Math.round(nanosTaken / 1_000_000d);
        }
        // >=50 ms?
        if (nanosTaken >= 1_000_000 * 50) {
            // -> Yes, >50ms, so use 1 decimal
            return Math.round(nanosTaken / 1_000_00d) / 10d;
        }
        // >=5 ms?
        if (nanosTaken >= 1_000_000 * 5) {
            // -> Yes, >5ms, so use 2 decimal
            return Math.round(nanosTaken / 1_000_0d) / 100d;
        }
        // E-> <5 ms
        // Use 3 decimals, but at least '0.0001' if round to zero, so as to point out that it is NOT 0.0d
        return Math.max(Math.round(nanosTaken / 1_000d) / 1_000d, 0.0001d);
    }
}
