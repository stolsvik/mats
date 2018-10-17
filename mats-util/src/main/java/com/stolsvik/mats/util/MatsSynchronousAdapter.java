package com.stolsvik.mats.util;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;

/**
 * This is a tool that should have a very limited set of use cases: Processes that by nature are synchronous, but needs
 * to call into the "MATS network" of services. This enables a thread to issue a request to a Mats endpoint, and then
 * synchronously wait for the service to reply. You should not really use it, ever! However, it provides a way to invoke
 * a Mats endpoint, and getting a {@link CompletableFuture} back - which is very convenient when e.g. implementing a
 * traditional blocking, synchronous HTTP request - or a public-static-void-main "script", or an integration test.
 * <p>
 * The class implements {@link AutoCloseable}, but this is meant for e.g. Spring environments which closes beans, and
 * does not imply that this class should be closed after each usage, i.e. do not use try-with-resources constructs. The
 * close method should be invoked when the application shuts down, not after e.g. each message.
 * <p>
 * This is built upon the MATS API, not tying into any of the implementation details.
 */
public class MatsSynchronousAdapter<R> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MatsSynchronousAdapter.class);

    public static <R> MatsSynchronousAdapter<R> create(MatsFactory matsFactory, MatsInitiator matsInitiator,
            String returnEndpointPrefix, Class<R> replyClass) {
        return new MatsSynchronousAdapter<>(matsFactory, matsInitiator, returnEndpointPrefix, replyClass);
    }

    /**
     * Reply class which will be returned from the {@link CompletableFuture} returned by
     * {@link #futureRequest(int, String, String, String, Object, InitiateLambda) futureRequest(...)}, which contains
     * the reply from the requested service, the {@link DetachedProcessContext} from the internal Stow stage which can
     * be used to access message-attached bytes or strings, and the number of millis between the request and the message
     * reply being received.
     * 
     * @param <R>
     *            the reply type.
     */
    public static class Reply<R> {
        private final DetachedProcessContext _processContext;
        private final R _reply;
        private final int _latency;

        Reply(DetachedProcessContext processContext, R reply, int latency) {
            _reply = reply;
            _processContext = processContext;
            _latency = latency;
        }

        public R getReply() {
            return _reply;
        }

        public DetachedProcessContext getProcessContext() {
            return _processContext;
        }

        public int getLatencyMillis() {
            return _latency;
        }
    }

    private final MatsInitiator _matsInitiator;
    private final String _completingSubscriptionEndpointId;
    private final MatsEndpoint<Void, String> _completingSubscriptionTerminator;

    private MatsSynchronousAdapter(MatsFactory matsFactory, MatsInitiator matsInitiator, String returnEndpointIdPrefix,
            Class<R> replyClass) {
        _matsInitiator = matsInitiator;

        _completingSubscriptionEndpointId = returnEndpointIdPrefix + ".private.SyncAdapter."
                + replyClass.getSimpleName() + "." + matsFactory.getFactoryConfig().getNodename();

        // :: Create the Thread that will timeout futures
        Thread timeoutThread = new Thread(this::timeouter,
                "MATS:SynchronousAdapterTimeoutThread:" + returnEndpointIdPrefix);
        timeoutThread.setDaemon(true);
        timeoutThread.start();

        // :: Create the SubscriptionTerminator that will complete futures.
        _completingSubscriptionTerminator = matsFactory.subscriptionTerminator(_completingSubscriptionEndpointId,
                String.class, replyClass,
                (context, correlationId, reply) -> {
                    // Find and remove the Future's promise, using the state String which acts as Correlation Id.
                    WaitingPromise<R> promise;
                    synchronized (_outstandingPromises) {
                        promise = _outstandingPromises.remove(correlationId);
                        // Notify the Timeout Thread so that it can re-calculate when to wake up.
                        _outstandingPromises.notifyAll();
                    }
                    // ?: Did we find the promise?
                    if (promise == null) {
                        // -> No, promise not present, so log a warning.
                        log.warn("WAITING PROMISE GONE: Got a reply from [" + context.getFromStageId()
                                + "], but the correlationId [" + correlationId + "] didn't map to a waiting promise."
                                + " It had probably already been completed exceptionally with "
                                + MatsFutureTimeoutException.class.getSimpleName() + " (thus, the future.get()-call"
                                + " got an ExecutionException)");
                        return;
                    }
                    // E-> Yes, promise is still present, so complete it.
                    int latency = (int) (System.currentTimeMillis() - promise._startMillis);
                    promise._future.complete(new Reply<>(context, reply, latency));

                    if (log.isDebugEnabled())
                        log.debug("COMPLETED FUTURE: We found the CorrelationId [" + correlationId
                                + "] for reply from [" + context.getFromStageId()
                                + "], and completed the Future with TraceId:[" + promise._traceId
                                + "] and FromId:[" + promise._fromId + "].");
                });
    }

    private boolean _runTimeoutThread = true;

    /**
     * Stops the Timeout Thread and cancels all outstanding Futures.
     */
    public void close() {
        synchronized (_outstandingPromises) {
            // Set the TimeoutThread to exit
            _runTimeoutThread = false;
            // Cancel all Futures (effectively completes with CancellationException)
            _outstandingPromises.values().forEach(waitingPromise -> waitingPromise._future.cancel(true));
            // Clear the Map (all are cancelled!)
            _outstandingPromises.clear();
            // Ping the Timeout Thread so that it notices that it should exit.
            _outstandingPromises.notifyAll();
        }
    }

    private final HashMap<String, WaitingPromise<R>> _outstandingPromises = new HashMap<>();

    private void timeouter() {
        log.info("Timeout Thread for MatsSynchronousAdapter [" + _completingSubscriptionEndpointId + "] started.");
        synchronized (_outstandingPromises) {
            while (_runTimeoutThread) {
                // :: Find the nearest Future to time out.
                long nextTimeout = _outstandingPromises.values().stream()
                        .mapToLong(value -> value._timeoutAt)
                        .min()
                        // If none, then 15 minute delay, to just say "hi, I am still here" if there are no futures.
                        .orElse(-1);

                // ?: Was there any outstanding Futures?
                if (nextTimeout == -1) {
                    // -> No, no outstanding Futures. Log this just to get some liveliness indication in log.
                    log.info("Currently no outstanding Futures.");
                    // Set next timeout to 15 minutes from now, so that we get periodic "I am alive!" log lines.
                    nextTimeout = System.currentTimeMillis() + (15 * 60 * 1000);
                }

                // :: Wait till the next Future is overdue, if none, then the check above ensure 15 mins
                long waitTime = Long.max(0, nextTimeout - System.currentTimeMillis());
                // ?: Are we already overdue for any Future?
                if (waitTime > 0) {
                    // -> No, we should wait
                    try {
                        // Wait a tad longer to account for possible a-bit-early wakeup.
                        _outstandingPromises.wait(waitTime + 25);
                    }
                    catch (InterruptedException e) {
                        log.warn("Interrupted - which is weird, since no interrupts are used here.");
                    }
                }

                // :: For all Futures that are overdue: resolveExceptionally(MatsFutureTimeoutException), and remove.
                _outstandingPromises.values().removeIf(promise -> {
                    // ?: Are we past the timeoutAt time?
                    if (System.currentTimeMillis() > promise._timeoutAt) {
                        try {
                            MDC.put("traceId", promise._traceId);
                            log.warn("Timed out Future [" + promise._future + "] with TraceId:[" + promise._traceId
                                    + "] and FromId: [" + promise._fromId + "].");
                        }
                        finally {
                            MDC.remove("traceId");
                        }
                        // -> Yes, so time it out by completeExceptionally, and return true to remove it.
                        promise._future.completeExceptionally(new MatsFutureTimeoutException(
                                "Future with TraceId:[" + promise._traceId + "] and"
                                        + " FromId:[" + promise._fromId + "]:"
                                        + " Waited more than the specified " + promise._timeoutMillis
                                        + " ms for this Future to complete."));
                        return true;
                    }
                    return false;
                });
            }
        }
        log.info("Timeout Thread for MatsSynchronousAdapter [" + _completingSubscriptionEndpointId + "] shut down.");
    }

    /**
     * A waiting promise will be {@link CompletableFuture#completeExceptionally(Throwable) completed execptionally} by
     * the internal timeout-thread if the timeout given in
     * {@link #futureRequest(int, String, String, String, Object, InitiateLambda) futureRequest(...)} is hit. If a
     * completableFuture.get() is waiting, it will receive a {@link ExecutionException}, with this
     * {@link MatsFutureTimeoutException} as cause.
     */
    public static class MatsFutureTimeoutException extends Exception {
        MatsFutureTimeoutException(String message) {
            super(message);
        }
    }

    /**
     * Base method, with all the features exposed.
     *
     * @param timeoutMillis
     *            how long time, in milliseconds, before the CompletableFuture completes exceptionally with a
     *            {@link MatsFutureTimeoutException}.
     * @param traceId
     *            The traceId for the request and resulting call flow - read guidelines for TraceIds in MATS API.
     * @param from
     *            who this is "from" - i.e. a virtual/synthetic endpointId that describes who is sending the request -
     *            for debugging and call tracing.
     * @param to
     *            the endpointId that should get the request.
     * @param requestDto
     *            the request object that should be sent to the requested endpoint.
     * @param messageCustomizer
     *            provides a way to customize the initiation, e.g. call {@link MatsInitiate#interactive()
     *            msg.interactive()}.
     *
     * @return a {@link CompletableFuture} which completes with a {@link Reply} instance, containing both the actual
     *         reply, and a {@link DetachedProcessContext} from which to retrieve meta information, "sticky properties",
     *         and any "side loaded" bytes or strings.
     */
    public CompletableFuture<Reply<R>> futureRequest(int timeoutMillis, String traceId, String from,
            String to, Object requestDto, InitiateLambda messageCustomizer) {

        // :: Should not send requests until we know that the completing endpoint has started (since it is a topic, any
        // messages sent to it while not started will just be dumped on the floor by the MQ Broker).
        _completingSubscriptionTerminator.waitForStarted();

        // :: Make the CompletableFuture that we'll return.
        CompletableFuture<Reply<R>> future = new CompletableFuture<>();

        // Create a Correlation Id based on the TraceId, but we don't trust the user to make a properly unique traceId.
        String correlationId = traceId + ":" + RandomString.randomCorrelationId();

        // Add the corresponding "WaitingPromise" for this CompletableFuture, and notify the timeoutThread.
        synchronized (_outstandingPromises) {
            _outstandingPromises.put(correlationId, new WaitingPromise<>(future, from,
                    correlationId, timeoutMillis, System.currentTimeMillis() + timeoutMillis));
            _outstandingPromises.notifyAll();
        }

        // Create the request.
        _matsInitiator.initiateUnchecked(msg -> {
            // Set up the message
            msg.traceId(traceId)
                    .to(to)
                    .from(from)
                    .replyToSubscription(_completingSubscriptionEndpointId, correlationId);

            // Invoke the customizer.
            messageCustomizer.initiate(msg);

            // Pack it off
            msg.request(requestDto);
        });

        return future;
    }

    /**
     * "GET"-variant of {@link #futureRequest(int, String, String, String, Object, InitiateLambda) futureRequest(...)}
     * that sets the {@link MatsInitiate#interactive() interactive} and {@link MatsInitiate#nonPersistent()} flags.
     * <p>
     * It is suitable for e.g. non-state-changing GET-style REST-endpoints that needs to hook into the Mats-system to
     * get some values for the return to the HTTP call: It should be fast (forget the commits to MQ backing store, i.e.
     * non-persistent) and be prioritized, since there is a human waiting (interactive-prioritization), and there is no
     * worries if the call sometimes (albeit very seldom) disappears (result of non-persistent).
     *
     * @param timeoutMillis
     *            how long time, in milliseconds, before the CompletableFuture completes exceptionally with a
     *            {@link MatsFutureTimeoutException}.
     * @param traceId
     *            The traceId for the request and resulting call flow - read guidelines for TraceIds in MATS API.
     * @param from
     *            who this is "from" - i.e. a virtual/synthetic endpointId that describes who is sending the request -
     *            for debugging and call tracing.
     * @param to
     *            the endpointId that should get the request.
     * @param requestDto
     *            the request object that should be sent to the requested endpoint.
     *
     * @return a {@link CompletableFuture} which completes with a {@link Reply} instance, containing both the actual
     *         reply, and a {@link DetachedProcessContext} from which to retrieve meta information, "sticky properties",
     *         and any "side loaded" bytes or strings.
     */
    public CompletableFuture<Reply<R>> interactiveNonPersistentRequest(int timeoutMillis, String traceId, String from,
            String to, Object requestDto) {
        return futureRequest(timeoutMillis, traceId, from, to, requestDto, msg -> {
            msg.interactive();
            msg.nonPersistent();
        });
    }

    /**
     * "GET"-variant of {@link #interactiveNonPersistentRequest(int, String, String, String, Object)
     * interactiveNonPersistentRequest(...)} that in addition to setting the the {@link MatsInitiate#interactive()
     * interactive} and {@link MatsInitiate#nonPersistent()} flag, also specifies a timeout of 5 seconds - which is
     * <i>way</i> more than what you should aim for wrt. the latencies of your services! (Less than 100 ms should be a
     * goal).
     * <p>
     * It is suitable for e.g. non-state-changing GET-style REST-endpoints that needs to hook into the Mats-system to
     * get some values for the return to the HTTP call: It should be fast (forget the commits to MQ backing store, i.e.
     * non-persistent) and be prioritized, since there is a human waiting (interactive-prioritization), and there is no
     * worries if the call sometimes (albeit very seldom) disappears (result of non-persistent).
     * 
     * @param traceId
     *            The traceId for the request and resulting call flow - read guidelines for TraceIds in MATS API.
     * @param from
     *            who this is "from" - i.e. a virtual/synthetic endpointId that describes who is sending the request -
     *            for debugging and call tracing.
     * @param to
     *            the endpointId that should get the request.
     * @param requestDto
     *            the request object that should be sent to the requested endpoint.
     *
     * @return a {@link CompletableFuture} which completes with a {@link Reply} instance, containing both the actual
     *         reply, and a {@link DetachedProcessContext} from which to retrieve meta information, "sticky properties",
     *         and any "side loaded" bytes or strings.
     */
    public CompletableFuture<Reply<R>> interactiveNonPersistentRequest(String traceId, String from, String to,
            Object requestDto) {
        return interactiveNonPersistentRequest(5000, traceId, from, to, requestDto);
    }

    /**
     * This is the Promise of the Future, representing the "backside" of the returned futures.
     */
    private static class WaitingPromise<R> {
        private final CompletableFuture<Reply<R>> _future;
        private final String _fromId;
        private final String _traceId;
        private final long _startMillis = System.currentTimeMillis();
        private final int _timeoutMillis;
        private final long _timeoutAt;

        public WaitingPromise(CompletableFuture<Reply<R>> future, String fromId,
                String traceId, int timeoutMillis, long timeoutAt) {
            _future = future;
            _fromId = fromId;
            _traceId = traceId;
            _timeoutMillis = timeoutMillis;
            _timeoutAt = timeoutAt;
        }
    }
}
