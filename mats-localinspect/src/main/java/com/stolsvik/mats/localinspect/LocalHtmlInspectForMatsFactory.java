package com.stolsvik.mats.localinspect;

import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.SortedMap;

import com.stolsvik.mats.MatsConfig;
import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.MatsStage.StageConfig;
import com.stolsvik.mats.api.intercept.MatsInterceptable;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.StageCompletedContext.ProcessResult;
import com.stolsvik.mats.localinspect.LocalStatsMatsInterceptor.EndpointStats;
import com.stolsvik.mats.localinspect.LocalStatsMatsInterceptor.IncomingMessageRepresentation;
import com.stolsvik.mats.localinspect.LocalStatsMatsInterceptor.InitiatorStats;
import com.stolsvik.mats.localinspect.LocalStatsMatsInterceptor.OutgoingMessageRepresentation;
import com.stolsvik.mats.localinspect.LocalStatsMatsInterceptor.StageStats;
import com.stolsvik.mats.localinspect.LocalStatsMatsInterceptor.StatsSnapshot;

/**
 * Will produce an "embeddable" HTML interface - notice that there are CSS ({@link #getStyleSheet(Writer)}), JavaScript
 * ({@link #getJavaScript(Writer)}) and HTML ({@link #createFactoryReport(Writer, boolean, boolean, boolean)
 * createFactoryReport(Writer,..)}) to include. If the {@link LocalStatsMatsInterceptor} is installed on the
 * {@link MatsFactory} implementing {@link MatsInterceptable}, it will include pretty nice "local statistics" for all
 * initiators, endpoints and stages.
 * <p />
 * Note: You are expected to {@link #create(MatsFactory) create} one instance of this class per MatsFactory, and keep
 * these around for the lifetime of the MatsFactories (i.e. for the JVM) - as in multiple singletons. Do not create one
 * per HTML request. The reason for this is that at a later point, this class might be extended with "active" features,
 * like stopping and starting endpoints, change the concurrency etc - at which point it might itself need active state,
 * e.g. for a feature like "stop this endpoint for 30 minutes".
 *
 * @author Endre Stølsvik 2021-03-25 - http://stolsvik.com/, endre@stolsvik.com
 */
public class LocalHtmlInspectForMatsFactory {

    public static LocalHtmlInspectForMatsFactory create(MatsFactory matsFactory) {
        return new LocalHtmlInspectForMatsFactory(matsFactory);
    }

    final MatsFactory matsFactory;

    LocalHtmlInspectForMatsFactory(MatsFactory matsFactory) {
        this.matsFactory = matsFactory;
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page, no matter how many
     * MatsFactories you display.
     */
    public void getStyleSheet(Writer writer) throws IOException {
        writer.write(".mats_report {\n"
                + "  font-family: sans-serif;\n"
                + "}\n");
        writer.write(".mats_report h2, .mats_report h3, .mats_report h4 {\n"
                + "  margin: 0px;\n"
                + "  font-family: sans-serif;\n"
                + "  display: inline;\n"
                + "}\n");
        writer.write(".mats_heading {\n"
                + "  font-size: 80%;\n"
                + "  display: block;\n"
                + "  margin: 0em 0em 0.5em 0em;\n"
                + "}\n");
        writer.write(".mats_info {\n"
                + "  font-size: 80%;\n"
                + "  margin: 0em 0em 0em 0.5em;\n"
                + "}\n");

        writer.write(".mats_factory {\n"
                + "  background: #f0f0f0;\n"
                + "}\n");
        writer.write(".mats_initiator {\n"
                + "  background: #e0f0e0;\n"
                + "}\n");
        writer.write(".mats_endpoint {\n"
                + "  background: #e0e0f0;\n"
                + "}\n");
        writer.write(".mats_stage {\n"
                + "  background: #f0f0f0;\n"
                + "}\n");
        // Box:
        writer.write(".mats_factory, .mats_initiator, .mats_endpoint, .mats_stage {\n"
                + "  box-shadow: 4px 4px 5px 4px rgba(0,0,0,0.37);\n"
                + "  border-radius: 5px;\n"
                + "  border: thin solid #a0a0a0;\n"
                + "  margin: 0.5em 0.5em 0.7em 0.5em;\n"
                + "  padding: 0.1em 0.5em 0.5em 0.5em;\n"
                + "}\n");

        writer.write(".mats_initiator, .mats_endpoint {\n"
                + "  margin: 0.5em 0.5em 2em 0.5em;\n"
                + "}\n");

        writer.write(".mats_hot {\n"
                + "  box-shadow: #FFF 0 -1px 4px, #ff0 0 -2px 10px, #ff8000 0 -10px 20px, red 0 -18px 40px, 5px 5px 15px 5px rgba(0,0,0,0);\n"
                + "  border: 0.2em solid red;\n"
                + "  background: #ECEFCF;\n"
                + "}\n");
    }

    /**
     * Note: The return from this method is static, and should only be included once per HTML page, no matter how many
     * MatsFactories you display.
     */
    public void getJavaScript(Writer writer) throws IOException {
        writer.write("");
    }

    public void createFactoryReport(Writer out, boolean includeInitiators,
            boolean includeEndpoints, boolean includeStages) throws IOException {

        LocalStatsMatsInterceptor localStats = null;
        if (matsFactory instanceof MatsInterceptable) {
            localStats = ((MatsInterceptable) matsFactory).getInitiationInterceptorSingleton(
                    LocalStatsMatsInterceptor.class).orElse(null);
        }

        FactoryConfig config = matsFactory.getFactoryConfig();
        out.write("<div class=\"mats_report mats_factory\">\n");
        out.write("<div class=\"mats_heading\">MatsFactory <h2>" + config.getName() + "</h2>\n");
        out.write(" - <b>Known number of CPUs:</b> " + config.getNumberOfCpus());
        out.write(" - <b>Concurrency:</b> " + formatConcurrency(config));
        out.write("</div>\n");
        out.write("<hr />\n");

        out.write("<div class=\"mats_info\">");
        out.write("<b>App:</b> " + config.getAppName() + " v." + config.getAppVersion());
        out.write(" - <b>Nodename:</b> " + config.getNodename());
        out.write(" - <b>Running:</b> " + config.isRunning());
        out.write(" - <b>Destination prefix:</b> \"" + config.getMatsDestinationPrefix() + "\"");
        out.write(" - <b>Trace key:</b> \"" + config.getMatsTraceKey() + "\"<br />\n");
        out.write((localStats != null
                ? "<b>Local Statistics collector present in MatsFactory!</b>"
                        + " (<code>" + LocalStatsMatsInterceptor.class.getSimpleName() + "</code> installed)"
                : "<b>Missing Local Statistics collector in MatsFactory - <code>"
                        + LocalStatsMatsInterceptor.class.getSimpleName()
                        + "</code> is not installed!</b>") + "</b><br />");
        out.write("</div>");
        if (includeInitiators) {
            for (MatsInitiator initiator : matsFactory.getInitiators()) {
                createInitiatorReport(out, initiator);
            }
        }

        if (includeEndpoints) {
            for (MatsEndpoint<?, ?> endpoint : matsFactory.getEndpoints()) {
                createEndpointReport(out, endpoint, includeStages);
            }
        }
        out.write("</div>\n");
    }

    public void createInitiatorReport(Writer out, MatsInitiator matsInitiator)
            throws IOException {
        LocalStatsMatsInterceptor localStats = null;
        MatsFactory parentFactory = matsInitiator.getParentFactory();
        if (parentFactory instanceof MatsInterceptable) {
            localStats = ((MatsInterceptable) parentFactory).getInitiationInterceptorSingleton(
                    LocalStatsMatsInterceptor.class).orElse(null);
        }

        out.write("<div class=\"mats_report mats_initiator\">\n");
        out.write("<div class=\"mats_heading\">Initiator <h3>" + matsInitiator.getName() + "</h3>\n");
        out.write("</div>\n");
        out.write("<hr />\n");
        out.write("<div class=\"mats_info\">\n");
        if (localStats != null) {
            Optional<InitiatorStats> initiatorStats_ = localStats.getInitiatorStats(matsInitiator);
            if (initiatorStats_.isPresent()) {
                InitiatorStats initiatorStats = initiatorStats_.get();
                StatsSnapshot stats = initiatorStats.getTotalExecutionTimeNanos();
                out.write("<b>Total initiation time:</b> " + statsNs(stats) + "<br />\n");
                SortedMap<String, SortedMap<OutgoingMessageRepresentation, Long>> outgoingMessageCounts = initiatorStats
                        .getOutgoingMessageCounts();
                if (outgoingMessageCounts.isEmpty()) {
                    out.write("<b>NO outgoing messages!</b><br />\n");
                }
                else if ((outgoingMessageCounts.size() == 1)
                        && (outgoingMessageCounts.get(outgoingMessageCounts.firstKey()).size() == 1)) {
                    // Singluar
                    String singleInitiatorId = outgoingMessageCounts.firstKey();
                    SortedMap<OutgoingMessageRepresentation, Long> singleInitiatorIdMsgs = outgoingMessageCounts.get(
                            singleInitiatorId);
                    OutgoingMessageRepresentation msg = singleInitiatorIdMsgs.firstKey();
                    Long count = singleInitiatorIdMsgs.get(msg);

                    out.write("<b>Outgoing messages:</b> " + count + " x from " + singleInitiatorId + ": " + msg
                            .getMessageType() + " to " + msg.getTo() + "<br />\n");
                }
                else {
                    // Multiple
                    out.write("<b>Outgoing messages:</b><br />\n");
                    for (Entry<String, SortedMap<OutgoingMessageRepresentation, Long>> initiatorToMsgs : outgoingMessageCounts
                            .entrySet()) {
                        String initiatorId = initiatorToMsgs.getKey();
                        out.write(".. From " + initiatorId + ":<br />\n");
                        for (Entry<OutgoingMessageRepresentation, Long> msgs : initiatorToMsgs
                                .getValue().entrySet()) {
                            OutgoingMessageRepresentation msg = msgs.getKey();
                            out.write(".... " + msgs.getValue() + " x " + msg.getMessageType() + " to " + msg.getTo()
                                    + "<br />\n");
                        }
                    }
                }
            }
        }
        out.write("</div>\n");
        out.write("</div>\n");
    }

    public void createEndpointReport(Writer out, MatsEndpoint<?, ?> matsEndpoint, boolean includeStages)
            throws IOException {
        LocalStatsMatsInterceptor localStats = null;
        MatsFactory parentFactory = matsEndpoint.getParentFactory();
        if (parentFactory instanceof MatsInterceptable) {
            localStats = ((MatsInterceptable) parentFactory).getStageInterceptorSingleton(
                    LocalStatsMatsInterceptor.class).orElse(null);
        }

        EndpointConfig<?, ?> config = matsEndpoint.getEndpointConfig();

        // Deduce type
        String type = config.getReplyClass() == void.class ? "Terminator" : "Endpoint";
        if ((matsEndpoint.getStages().size() == 1) && (config.getReplyClass() != void.class)) {
            type = "Single " + type;
        }
        if (matsEndpoint.getStages().size() > 1) {
            type = "MultiStage " + type;
        }
        if (matsEndpoint.getEndpointConfig().isSubscription()) {
            type = "Subscription " + type;
        }

        StatsSnapshot totExecSnapshot = null;
        if (localStats != null) {
            Optional<EndpointStats> endpointStats_ = localStats.getEndpointStats(matsEndpoint);
            if (endpointStats_.isPresent()) {
                EndpointStats endpointStats = endpointStats_.get();
                totExecSnapshot = endpointStats.getTotalEndpointProcessingNanos();
            }
        }

        // If we have snapshot, and the 99.5% percentile is too high, add the "mats hot" class.
        String hot = (totExecSnapshot != null) && (totExecSnapshot.get995thPercentile() > 1000_000_000d)
                ? " mats_hot"
                : "";

        out.write("<div class=\"mats_report mats_endpoint" + hot + "\">\n");

        out.write("<div class=\"mats_heading\">" + type + " <h3>" + config.getEndpointId() + "</h3>");
        out.write(" - <b>Incoming:</b> <code>" + config.getIncomingClass().getSimpleName() + "</code>\n");
        out.write(" - <b>Reply:</b> <code>" + config.getReplyClass().getSimpleName() + "</code>\n");
        out.write(" - <b>State:</b> <code>" + config.getStateClass().getSimpleName() + "</code>\n");
        out.write(" - <b>Concurrency:</b> " + formatConcurrency(config) + "\n");
        out.write("</div>\n");
        out.write("<hr />\n");

        out.write("<div class=\"mats_info\">\n");
        // out.write("Creation debug info: ### <br />\n");
        // out.write("Worst stage duty cycle: ### <br />\n");
        if (totExecSnapshot != null) {
            out.write("<b>Total endpoint time:</b> " + statsNs(totExecSnapshot) + "<br /><br />\n");
        }
        out.write("</div>");

        if (includeStages) {
            for (MatsStage<?, ?, ?> stage : matsEndpoint.getStages()) {

                // :: Time between stages
                if (localStats != null) {
                    Optional<StageStats> stageStats_ = localStats.getStageStats(stage);
                    if (stageStats_.isPresent()) {
                        StageStats stageStats = stageStats_.get();
                        StatsSnapshot stats = stageStats.getBetweenStagesTimeNanos();
                        // ?: Do we have Between-stats? (Do not have for initial stage).
                        if (stats != null) {
                            out.write("<div class=\"mats_info\"><b>Time between:</b> " + statsNs(stats) + "</div>\n");
                        }
                    }
                }

                createStageReport(out, stage);
            }
        }
        out.write("</div>\n");
    }

    public void createStageReport(Writer out, MatsStage<?, ?, ?> matsStage) throws IOException {
        LocalStatsMatsInterceptor localStats = null;
        MatsFactory parentFactory = matsStage.getParentEndpoint().getParentFactory();
        if (parentFactory instanceof MatsInterceptable) {
            localStats = ((MatsInterceptable) parentFactory).getStageInterceptorSingleton(
                    LocalStatsMatsInterceptor.class).orElse(null);
        }

        StatsSnapshot totExecSnapshot = null;
        StageStats stageStats = null;
        if (localStats != null) {
            Optional<StageStats> stageStats_ = localStats.getStageStats(matsStage);
            if (stageStats_.isPresent()) {
                stageStats = stageStats_.get();
                totExecSnapshot = stageStats.getTotalExecutionTimeNanos();
            }
        }

        StageConfig<?, ?, ?> config = matsStage.getStageConfig();

        // If we have snapshot, and the 99.5% percentile is too high, add the "mats hot" class.
        String hot = (totExecSnapshot != null) && (totExecSnapshot.get995thPercentile() > 500_000_000d)
                ? " mats_hot"
                : "";
        out.write("<div class=\"mats_report mats_stage" + hot + "\">\n");
        out.write("<div class=\"mats_heading\">Stage <h4>" + config.getStageId() + "</h4>\n");
        out.write(" - <b>Incoming:</b> <code>" + config.getIncomingClass().getSimpleName() + "</code>\n");
        out.write(" - <b>Concurrency:</b> " + formatConcurrency(config) + "\n");
        out.write(" - <b>Running stage processors:</b> " + config.getRunningStageProcessors() + "\n");
        out.write("</div>\n");
        out.write("<hr />\n");

        out.write("<div class=\"mats_info\">\n");
        // out.write("Creation debug info: ### <br />\n");
        // out.write("Duty cycle: ### <br />\n");
        // out.write("Oldest reported \"check-in\" for stage procs: ### seconds ago."
        // + " <b>Stuck stage procs: ###</b><br />\n");
        // out.write("<b>For terminators: - total times per outgoing initiation to difference services"
        // + " (use extra-state), should make things better for MatsFuturizer</b><br />\n");
        if (stageStats != null) {

            Map<IncomingMessageRepresentation, Long> incomingMessageCounts = stageStats.getIncomingMessageCounts();
            if (incomingMessageCounts.isEmpty()) {
                out.write("<b>NO incoming messages!</b>\n");
            }
            else if (incomingMessageCounts.size() == 1) {
                out.write("<b>Incoming messages:</b> ");
            }
            else {
                out.write("<b>Incoming messages (" + totExecSnapshot.getNumObservations() + "):</b><br />\n");
            }
            for (Entry<IncomingMessageRepresentation, Long> entry : incomingMessageCounts.entrySet()) {
                IncomingMessageRepresentation msg = entry.getKey();
                out.write(entry.getValue() + " x " + msg.getMessageType()
                        + " from <i>" + msg.getFromStageId() + "</i> <b>@</b> " + msg.getFromAppName()
                        + " &mdash; (init:<i>" + msg.getInitiatorId() + "</i> <b>@</b> " + msg
                                .getInitiatingAppName()
                        + ")<br />");
            }

            out.write("<b>Total stage time:</b> " + statsNs(totExecSnapshot) + "<br />\n");

            // :: ProcessingResults
            SortedMap<ProcessResult, Long> processResultCounts = stageStats.getProcessResultCounts();
            if (processResultCounts.isEmpty()) {
                out.write("<b>NO processing results!</b><br />\n");
            }
            else if (processResultCounts.size() == 1) {
                ProcessResult processResult = processResultCounts.firstKey();
                Long count = processResultCounts.get(processResult);
                out.write("<b>Processing results:</b>\n");
            }
            else {
                out.write("<b>Processing results:</b><br />\n");
            }
            for (Entry<ProcessResult, Long> entry : processResultCounts.entrySet()) {
                ProcessResult processResult = entry.getKey();
                out.write(entry.getValue() + " x " + processResult + "<br />\n");
            }

            // :: Outgoing messages
            SortedMap<OutgoingMessageRepresentation, Long> outgoingMessageCounts = stageStats
                    .getOutgoingMessageCounts();
            long sumOutMsgs = outgoingMessageCounts.values().stream().mapToLong(Long::longValue).sum();
            if (outgoingMessageCounts.isEmpty()) {
                out.write("<b>NO outgoing messages!</b><br />\n");
            }
            else if (outgoingMessageCounts.size() == 1) {
                out.write("<b>Outgoing messages:</b> \n");

            }
            else {
                out.write("<b>Outgoing messages (" + sumOutMsgs + "):</b><br />\n");
            }

            for (Entry<OutgoingMessageRepresentation, Long> entry : outgoingMessageCounts.entrySet()) {
                OutgoingMessageRepresentation msg = entry.getKey();
                out.write(entry.getValue() + " x " + msg.getMessageType() + " to " + msg.getTo() + "<br />\n");
            }
        }
        out.write("</div>\n");
        out.write("</div>\n");
    }

    String formatConcurrency(MatsConfig config) {
        return config.getConcurrency() + (config.isConcurrencyDefault() ? " <i>(inherited)</i>"
                : " <i><b>(explicitly set)</b></i>");
    }

    static String statsNs(StatsSnapshot snapshot) {
        double sd = snapshot.getStdDev();
        double avg = snapshot.getAverage();
        return "<b>x\u0304:</b>" + nano_to_ms3(avg)
                + " <b><i>s</i>:</b>" + nano_to_ms3(sd)
                + " <b><i>2s</i>:</b>[" + nano_to_ms3(avg - 2 * sd) + ", " + nano_to_ms3(avg + 2 * sd) + "]"
                + " - <b><span style=\"font-size:80%\">min:</span></b>" + nano_to_ms3(snapshot.getMin())
                + " <b><sup style=\"font-size:80%\">max:</sup></b>" + nano_to_ms3(snapshot.getMax())
                + " &mdash; percentiles <b>50%:</b>" + nano_to_ms3(snapshot.getMedian())
                + ", <b>75%:</b>" + nano_to_ms3(snapshot.get75thPercentile())
                + ", <b>95%:</b>" + nano_to_ms3(snapshot.get95thPercentile())
                + ", <b>98%:</b>" + nano_to_ms3(snapshot.get98thPercentile())
                + ", <b>99%:</b>" + nano_to_ms3(snapshot.get99thPercentile())
                + ", <b>99.5%:</b>" + nano_to_ms3(snapshot.get995thPercentile())
                + " &mdash; <i>number of samples: " + snapshot.getSamples().length
                + ", out of observations:" + snapshot.getNumObservations() + "</i>";
    }

    static final DecimalFormatSymbols NF_SYMBOLS;
    static final DecimalFormat NF_0_DECIMALS;
    static final DecimalFormat NF_1_DECIMALS;
    static final DecimalFormat NF_2_DECIMALS;
    static final DecimalFormat NF_3_DECIMALS;
    static {
        NF_SYMBOLS = new DecimalFormatSymbols(Locale.US);
        NF_SYMBOLS.setDecimalSeparator('.');
        NF_SYMBOLS.setGroupingSeparator('\u202f');

        NF_0_DECIMALS = new DecimalFormat("<i>#,##0</i>");
        NF_0_DECIMALS.setMaximumFractionDigits(0);
        NF_0_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);

        NF_1_DECIMALS = new DecimalFormat("#,##0.0");
        NF_1_DECIMALS.setMaximumFractionDigits(1);
        NF_1_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);

        NF_2_DECIMALS = new DecimalFormat("#,##0.00");
        NF_2_DECIMALS.setMaximumFractionDigits(2);
        NF_2_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);

        NF_3_DECIMALS = new DecimalFormat("#,##0.000");
        NF_3_DECIMALS.setMaximumFractionDigits(3);
        NF_3_DECIMALS.setDecimalFormatSymbols(NF_SYMBOLS);
    }

    static String nano_to_ms3(double nanos) {
        if (Double.isNaN(nanos)) {
            return "NaN";
        }
        if (nanos == 0d) {
            return "0";
        }
        // >=500 ms?
        if (nanos >= 1_000_000L * 500) {
            // -> Yes, >500ms, so chop off fraction entirely, e.g. 612
            return NF_0_DECIMALS.format(Math.round(nanos / 1_000_000d));
        }
        // >=50 ms?
        if (nanos >= 1_000_000L * 50) {
            // -> Yes, >50ms, so use 1 decimal, e.g. 61.2
            return NF_1_DECIMALS.format(Math.round(nanos / 100_000d) / 10d);
        }
        // >=5 ms?
        if (nanos >= 1_000_000L * 5) {
            // -> Yes, >5ms, so use 2 decimal, e.g. 6.12
            return NF_2_DECIMALS.format(Math.round(nanos / 10_000d) / 100d);
        }
        // Negative? (Can happen when we to 'avg - 2 x std.dev', the result becomes negative)
        if (nanos < 0) {
            // -> Negative, so use three digits
            return NF_3_DECIMALS.format(Math.round(nanos / 1_000d) / 1_000d);
        }
        // E-> <5 ms
        // Use 3 decimals, e.g. 0.612
        double round = Math.round(nanos / 1_000d) / 1_000d;
        // ?: However, did we round to zero?
        if (round == 0) {
            // -> Yes, round to zero, so show special case
            return "~>0";
        }
        return NF_3_DECIMALS.format(round);
    }
}
