package com.stolsvik.mats.intercept.micrometer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
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
 * <b>Note: This interceptor (Micrometer Metrics) has special support in <code>JmsMatsFactory</code>: If present on the
 * classpath, it is automatically installed using the {@link #install(MatsInterceptable)} install method.</b> This
 * implies that it employs the {@link Metrics#globalRegistry Micrometer 'globalRegistry'}. If you rather want to supply
 * a specific registry, then install a different instance using the {@link #install(MatsInterceptable, MeterRegistry)}
 * method - the <code>JmsMatsFactory</code> will then remove the automatically installed, since it implements the
 * special marker-interface {@link MatsMetricsInterceptor} of which there can only be one instance installed.
 *
 * @author Endre Stølsvik - 2021-02-07 12:45 - http://endre.stolsvik.com
 */
public class MatsMicrometerInterceptor
        implements MatsMetricsInterceptor, MatsInitiateInterceptor, MatsStageInterceptor {

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
        matsInterceptableMatsFactory.addInitiationInterceptor(metrics);
        matsInterceptableMatsFactory.addStageInterceptor(metrics);
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
        matsInterceptable.addInitiationInterceptor(GLOBAL_REGISTRY_INSTANCE);
        matsInterceptable.addStageInterceptor(GLOBAL_REGISTRY_INSTANCE);
        return GLOBAL_REGISTRY_INSTANCE;
    }

    @Override
    public void initiateCompleted(InitiateCompletedContext ctx) {
        List<MatsSentOutgoingMessage> outgoingMessages = ctx.getOutgoingMessages();

        if (!outgoingMessages.isEmpty()) {
            MatsSentOutgoingMessage firstMessage = outgoingMessages.get(0);
            String fromId = firstMessage.getFrom();
            Timer timer_TotalTime = Timer.builder("mats.init.total")
                    .tag("initiator", ctx.getInitiator().getName())
                    .tag("from", fromId)
                    .description("Total time taken to execute initialization")
                    .register(_meterRegistry);
            timer_TotalTime.record(ctx.getTotalExecutionNanos(), TimeUnit.NANOSECONDS);

            Timer timer_DbCommit = Timer.builder("mats.init.dbcommit")
                    .tag("initiator", ctx.getInitiator().getName())
                    .tag("from", fromId)
                    .description("Time taken to commit database")
                    .register(_meterRegistry);
            timer_DbCommit.record(ctx.getDbCommitNanos(), TimeUnit.NANOSECONDS);

            Timer timer_MsgSend = Timer.builder("mats.init.msgsend")
                    .tag("initiator", ctx.getInitiator().getName())
                    .tag("from", fromId)
                    .description("Time taken (sum) to produce and send messages to message system")
                    .register(_meterRegistry);
            timer_MsgSend.record(ctx.getSumMessageSystemProductionAndSendNanos(), TimeUnit.NANOSECONDS);

            Timer timer_MsgCommit = Timer.builder("mats.init.msgcommit")
                    .tag("initiator", ctx.getInitiator().getName())
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
    }

    @Override
    public void stageReceived(StageReceivedContext ctx) {
        ProcessContext<Object> processContext = ctx.getProcessContext();

        long sumNanosPieces = ctx.getMessageSystemDeconstructNanos()
                + ctx.getEnvelopeDecompressionNanos()
                + ctx.getEnvelopeDeserializationNanos()
                + ctx.getMessageAndStateDeserializationNanos();

        // TODO: Interesting to metrics?
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
    }
}
