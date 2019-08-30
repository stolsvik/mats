package com.stolsvik.mats.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.DetachedProcessContext;
import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;

/**
 * Note: WIP.
 *
 * @author Endre Stølsvik 2019-08-25 20:35 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsFuturizer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MatsFuturizer.class);
    private String LOG_PREFIX = "#MATS-UTIL# ";

    /**
     * Creates a MatsFuturizer, <b>and you should only need one per MatsFactory</b> (which again mostly means one per
     * application or micro-service or JVM). The number of threads in the future-completer-pool is
     * {@link Runtime#availableProcessors()} for "corePoolSize" (i.e. "min") and availableProcessors * 5 for
     * "maximumPoolSize" (i.e. max).
     * 
     * @param matsFactory
     *            The underlying {@link MatsFactory} on which outgoing messages will be sent, and on which the receiving
     *            {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *            SubscriptionTerminator} will be created.
     * @param endpointIdPrefix
     *            the first part of the endpointId, which typically should be some "class-like" construct denoting the
     *            service name, like "OrderService" or "InventoryService".
     * @return the {@link MatsFuturizer}, which is tied to a newly created
     *         {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *         SubscriptionTerminator}.
     */
    public static MatsFuturizer createMatsFuturizer(MatsFactory matsFactory, String endpointIdPrefix) {
        int cpus = Runtime.getRuntime().availableProcessors();
        return createMatsFuturizer(matsFactory, endpointIdPrefix, cpus, cpus * 5);
    }

    /**
     * Creates a MatsFuturizer, <b>and you should only need one per MatsFactory</b> (which again mostly means one per
     * application or micro-service or JVM). With this constructor you can specify the number of threads in the
     * future-completer-pool with the parameters "corePoolSize" and "maximumPoolSize" threads, which effectively means
     * min and max.
     *
     * @param matsFactory
     *            The underlying {@link MatsFactory} on which outgoing messages will be sent, and on which the receiving
     *            {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *            SubscriptionTerminator} will be created.
     * @param endpointIdPrefix
     *            the first part of the endpointId, which typically should be some "class-like" construct denoting the
     *            service name, like "OrderService" or "InventoryService".
     * @param corePoolSize
     *            the minimum number of threads in the future-completer-pool of threads.
     * @param maximumPoolSize
     *            the maximum number of threads in the future-completer-pool of threads.
     * @return the {@link MatsFuturizer}, which is tied to a newly created
     *         {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *         SubscriptionTerminator}.
     */
    public static MatsFuturizer createMatsFuturizer(MatsFactory matsFactory, String endpointIdPrefix,
            int corePoolSize, int maximumPoolSize) {
        return new MatsFuturizer(matsFactory, endpointIdPrefix, corePoolSize, maximumPoolSize);
    }

    protected final MatsFactory _matsFactory;
    protected final MatsInitiator _matsInitiator;
    protected final String _terminatorEndpointId;
    protected final ThreadPoolExecutor _threadPool;
    protected final MatsEndpoint<Void, String> _replyHandlerEndpoint;

    protected MatsFuturizer(MatsFactory matsFactory, String endpointIdPrefix, int corePoolSize, int maximumPoolSize) {
        _matsFactory = matsFactory;
        _matsInitiator = matsFactory.getDefaultInitiator();
        _terminatorEndpointId = endpointIdPrefix + ".private.Futurizer."
                + _matsFactory.getFactoryConfig().getNodename();
        _threadPool = _newThreadPool(corePoolSize, maximumPoolSize);
        _replyHandlerEndpoint = _matsFactory.subscriptionTerminator(_terminatorEndpointId, String.class,
                MatsObject.class,
                this::handleRepliesForPromises);
        _startTimeouterThread();
    }

    public static class Reply<T> {
        public final DetachedProcessContext context;
        public final T reply;
        public final long initiatedTimestamp;

        public Reply(DetachedProcessContext context, T reply, long initiatedTimestamp) {
            this.context = context;
            this.reply = reply;
            this.initiatedTimestamp = initiatedTimestamp;
        }
    }

    /**
     * This exception is raised through the {@link CompletableFuture} if the timeout specified when getting the
     * {@link CompletableFuture} is reached (You get such CompletableFutures using the
     * {@link #futurizeInteractiveUnreliable(String, String, String, int, Class, Object) futurizeXYZ(..)} methods). The
     * exception is passed to the waiter on the future by {@link CompletableFuture#completeExceptionally(Throwable)}.
     */
    public static class MatsFuturizerTimeoutException extends RuntimeException {
        public MatsFuturizerTimeoutException(String message) {
            super(message);
        }
    }

    public <T> CompletableFuture<Reply<T>> futurizeInteractiveUnreliable(String traceId, String from, String to,
            int timeoutMillis,
            Class<T> replyClass, Object request) {
        return futurizeGeneric(traceId, from, to, timeoutMillis, replyClass, request,
                msg -> msg.interactive().nonPersistent());
    }

    public <T> CompletableFuture<Reply<T>> futurizeGeneric(String traceId, String from, String to, int timeoutMillis,
            Class<T> replyClass, Object request, InitiateLambda extraMessageInit) {
        Promise<T> promise = _createPromise(traceId, from, replyClass, timeoutMillis);
        _assertFuturizerRunning();
        _enqueuePromise(promise);
        _sendRequestToFulfillPromise(from, to, traceId, request, extraMessageInit, promise);
        return promise._future;
    }

    // ===== Internal classes and methods, can be overridden if you want to make a customized MatsFuturizer

    protected static class Promise<T> implements Comparable<Promise<?>> {
        private final String _traceId;
        private final String _correlationId;
        private final String _from;
        private final long _initiatedTimestamp;
        private final long _timeoutTimestamp;
        private final Class<T> _replyClass;
        private final CompletableFuture<Reply<T>> _future;

        public Promise(String traceId, String correlationId, String from, long initiatedTimestamp,
                long timeoutTimestamp, Class<T> replyClass, CompletableFuture<Reply<T>> future) {
            _traceId = traceId;
            _correlationId = correlationId;
            _from = from;
            _initiatedTimestamp = initiatedTimestamp;
            _timeoutTimestamp = timeoutTimestamp;
            _replyClass = replyClass;
            _future = future;
        }

        @Override
        public int compareTo(Promise<?> o) {
            // ?: Are timestamps equal?
            if (this._timeoutTimestamp == o._timeoutTimestamp) {
                // -> Yes, timestamps equal, so compare by correlationId.
                return this._correlationId.compareTo(o._correlationId);
            }
            // "signum", but zero is handled above.
            return this._timeoutTimestamp - o._timeoutTimestamp > 0 ? +1 : -1;
        }
    }

    protected ThreadPoolExecutor _newThreadPool(int corePoolSize, int maximumPoolSize) {
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 5L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), _newThreadFactory());
    }

    protected ThreadFactory _newThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return r -> new Thread(r, "MatsFuturizer completer #" + threadNumber.getAndIncrement());
    }

    protected <T> Promise<T> _createPromise(String traceId, String from, Class<T> replyClass, int timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("Timeout cannot be zero or negative [" + timeoutMillis + "].");
        }
        String correlationId = RandomString.randomCorrelationId();
        long timestamp = System.currentTimeMillis();
        CompletableFuture<Reply<T>> future = new CompletableFuture<>();
        return new Promise<>(traceId, correlationId, from, timestamp, timestamp + timeoutMillis, replyClass, future);
    }

    protected <T> void _enqueuePromise(Promise<T> promise) {
        synchronized (_correlationIdToPromiseMap) {
            // This is the lookup that the reply-handler uses to get to the promise from the correlationId.
            _correlationIdToPromiseMap.put(promise._correlationId, promise);
            // This is the priority queue that the timeouter-thread uses to get the next Promise to timeout.
            _timeoutSortedPromises.add(promise);
            // ?: Have the earliest Promise to timeout changed by adding this Promise?
            if (_nextInLineToTimeout != _timeoutSortedPromises.peek()) {
                // -> Yes, this was evidently earlier than the one we had "next in line", so notify the timeouter-thread
                // that a new promise was entered, to re-evaluate "next to timeout".
                _correlationIdToPromiseMap.notifyAll();
            }
        }
    }

    protected volatile boolean _replyHandlerEndpointStarted;

    protected void _assertFuturizerRunning() {
        // ?: Have we already checked that the reply endpoint is running?
        if (!_replyHandlerEndpointStarted) {
            // -> No, so wait for it to start now
            boolean started = _replyHandlerEndpoint.waitForStarted(30_000);
            // ?: Did it start?
            if (!started) {
                // -> No, so that's bad.
                throw new IllegalStateException("The Reply Handler SubscriptionTerminator Endpoint would not start.");
            }
            // Shortcut this question forever after.
            _replyHandlerEndpointStarted = true;
        }
        // ?: Have we already shut down?
        if (!_runFlag) {
            // -> Yes, shut down, so that's bad.
            throw new IllegalStateException("This MatsFuturizer [" + _terminatorEndpointId + "] is shut down.");
        }
    }

    protected <T> void _sendRequestToFulfillPromise(String from, String endpointId, String traceId, Object request,
            InitiateLambda extraMessageInit, Promise<T> promise) {
        _matsInitiator.initiateUnchecked(msg -> {
            // Stash in the standard stuff
            msg.traceId(traceId)
                    .from(from)
                    .to(endpointId)
                    .replyToSubscription(_terminatorEndpointId, promise._correlationId);
            // Stash up with any extra initialization stuff
            extraMessageInit.initiate(msg);
            // Do the request.
            msg.request(request);
        });
    }

    // Synchronized on itself
    protected final HashMap<String, Promise<?>> _correlationIdToPromiseMap = new HashMap<>();
    // Synchronized on the HashMap above (i.e. all three are synchronized on the HashMap).
    protected final PriorityQueue<Promise<?>> _timeoutSortedPromises = new PriorityQueue<>();
    // Synchronized on the HashMap above (i.e. all three are synchronized on the HashMap).
    protected Promise<?> _nextInLineToTimeout;

    protected void handleRepliesForPromises(ProcessContext<Void> context, String correlationId,
            MatsObject replyObject) {
        // Immediately pick this out of the map & queue
        Promise<?> promise;
        synchronized (_correlationIdToPromiseMap) {
            promise = _correlationIdToPromiseMap.remove(correlationId);
            if (promise != null) {
                _timeoutSortedPromises.remove(promise);
            }
        }
        // ?: Did we still have the Promise?
        if (promise == null) {
            // -> Promise gone, log on WARN and exit.
            log.warn(LOG_PREFIX + "Got reply from [" + context
                    .getFromStageId() + "] for Future with traceId:[" + context.getTraceId()
                    + "], but the Promise had timed out. It was sent [" + (System.currentTimeMillis()
                            - promise._initiatedTimestamp) + " ms] ago, while the timeout was ["
                    + (promise._timeoutTimestamp - promise._initiatedTimestamp) + " ms].");
            return;
        }

        // ----- We have Promise, and shall now fulfill it. Send off to pool thread.

        _threadPool.execute(() -> {
            try {
                uncheckedComplete(context, replyObject, promise);
            }
            catch (Throwable t) {
                log.error(LOG_PREFIX + "Got problems completing Future initiated from [" + promise._from
                        + "] with reply from [" + context.getFromStageId()
                        + "] with traceId:[" + context.getTraceId() + "]");
            }
        });
    }

    @SuppressWarnings("unchecked")
    protected void uncheckedComplete(ProcessContext<Void> context, MatsObject replyObject, Promise<?> promise) {
        Object replyInReplyClass = replyObject.toClass(promise._replyClass);
        Reply<?> tReply = new Reply<>(context, replyInReplyClass, promise._initiatedTimestamp);
        promise._future.complete((Reply) tReply);
    }

    protected volatile boolean _runFlag = true;

    protected void _startTimeouterThread() {
        Runnable timeouter = () -> {
            log.info("MatsFuturizer Timeouter-thread: Started!");
            while (_runFlag) {
                List<Promise<?>> promisesToTimeout = new ArrayList<>();
                synchronized (_correlationIdToPromiseMap) {
                    while (_runFlag) {
                        try {
                            long sleepMillis;
                            long now = System.currentTimeMillis();
                            Promise<?> peekPromise = _timeoutSortedPromises.peek();
                            if (peekPromise != null) {
                                // ?: Is this Promise overdue? I.e. current time has passed timeout timestamp of
                                // promise.
                                if (now >= peekPromise._timeoutTimestamp) {
                                    // -> Yes, timed out. remove from both collections
                                    // It is the first, since it is the object we peeked at.
                                    _timeoutSortedPromises.remove();
                                    // Remove explicitly by CorrelationId.
                                    _correlationIdToPromiseMap.remove(peekPromise._correlationId);
                                    // Put it in the list to timeout
                                    promisesToTimeout.add(peekPromise);
                                    // Check next in line
                                    continue;
                                }
                                // E-> It was not overdue, so see how long to sleep.
                                sleepMillis = peekPromise._timeoutTimestamp - now;
                                _nextInLineToTimeout = peekPromise;
                            }
                            else {
                                // Chill for a while, then just check to be on the safe side..
                                sleepMillis = 30_000;
                                // No Promises waiting.
                                _nextInLineToTimeout = null;
                            }
                            // ?: Do we have any Promises to timeout?
                            if (!promisesToTimeout.isEmpty()) {
                                // -> Yes, Promises to timeout - exit out of synch and inner run-loop to do that.
                                break;
                            }
                            // ----- We've found a new sleep time, go sleep.
                            // Now go to sleep, waiting for signal from "new element added" or close()
                            _correlationIdToPromiseMap.wait(sleepMillis);
                        }
                        // :: Protection against bad code - catch-all Throwables in hope that it will auto-correct.
                        catch (Throwable t) {
                            log.error("Got an unexpected Throwable in the promise-timeouter-thread."
                                    + " Loop and check whether to exit.", t);
                            // If exiting, do it now.
                            if (!_runFlag) {
                                break;
                            }
                            // :: Protection against bad code - sleep a tad to not tight-loop.
                            try {
                                Thread.sleep(10_000);
                            }
                            catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

                // ----- This is outside the synch block

                // :: Timing out Promises that was found to be overdue.
                for (Promise<?> promise : promisesToTimeout) {
                    MDC.put("traceId", promise._traceId);
                    log.info("Timing out Promise/Future initiated from [" + promise._from + "] with traceId ["
                            + promise._traceId + "].");
                    MDC.remove("traceId");
                    _threadPool.execute(() -> {
                        try {
                            promise._future.completeExceptionally(new MatsFuturizerTimeoutException(
                                    "The Promise/Future timed out! It was initiated from:[" + promise._from
                                            + "] with traceId:[" + promise._traceId + "]."
                                            + " Initiation was [" + (System.currentTimeMillis()
                                                    - promise._initiatedTimestamp) + " ms] ago, and its specified"
                                            + " timeout was:[" + (promise._timeoutTimestamp
                                                    - promise._initiatedTimestamp) + "]."));
                        }
                        catch (Throwable t) {
                            MDC.put("traceId", promise._traceId);
                            log.error(LOG_PREFIX + "Got problems timing out Promise/Future initiated from:["
                                    + promise._from + "] with traceId:[" + promise._traceId + "]", t);
                            MDC.remove("traceId");
                        }
                    });
                }
                promisesToTimeout.clear();
                // .. will now loop into the synch block again.
            }
            log.info("MatsFuturizer Timeouter-thread: We got asked to exit, and that we do!");
        };
        new Thread(timeouter, "MatsFuturizer Timeouter").start();
    }

    /**
     * Closes the MatsFuturizer. Notice: Spring will also notice this method if the MatsFuturizer is registered as a
     * <code>@Bean</code>, and will register it as a destroy method.
     */
    public void close() {
        log.info("MatsFuturizer.close() invoked: Shutting down reply-handler-endpoint, future-completer-threadpool,"
                + " timeouter-thread, and cancelling any outstanding futures.");
        _runFlag = false;
        _replyHandlerEndpoint.stop(5000);
        _threadPool.shutdown();
        // :: Find all remainging Promises, and notify Timeouter-thread that we're dead.
        List<Promise<?>> promisesToCancel = new ArrayList<>();
        synchronized (_correlationIdToPromiseMap) {
            promisesToCancel.addAll(_timeoutSortedPromises);
            // Clear the collections, just to have a clear conscience.
            _timeoutSortedPromises.clear();
            _correlationIdToPromiseMap.clear();
            // Notify the Timeouter-thread that shit is going down.
            _correlationIdToPromiseMap.notifyAll();
        }
        // :: Cancel all outstanding Promises.
        for (Promise<?> promise : promisesToCancel) {
            try {
                promise._future.cancel(true);
            }
            catch (Throwable t) {
                MDC.put("traceId", promise._traceId);
                log.error(LOG_PREFIX + "Got problems cancelling (due to shutdown) Promise/Future initiated from:["
                        + promise._from + "] with traceId:[" + promise._traceId + "]", t);
                MDC.remove("traceId");
            }
        }
    }
}
