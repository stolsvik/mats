package com.stolsvik.mats.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;

/**
 * Note: Kick-ass. TODO: Make better JavaDoc
 *
 * @author Endre Stølsvik 2019-08-25 20:35 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsFuturizer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MatsFuturizer.class);
    private String LOG_PREFIX = "#MATS-UTIL# ";

    /**
     * Creates a MatsFuturizer, <b>and you should only need one per MatsFactory</b> (which again mostly means one per
     * application or micro-service or JVM). The number of threads in the future-completer-pool is what
     * {@link FactoryConfig#getConcurrency() matsFactory.getFactoryConfig().getConcurrency()} returns at creation time x
     * 4 for "corePoolSize", but at least 5, (i.e. "min"), and concurrency * 20, but at least 100, for "maximumPoolSize"
     * (i.e. max). The pool is set up to let non-core threads expire after 5 minutes. The maximum number of outstanding
     * promises is set to 50k.
     *
     * @param matsFactory
     *            the underlying {@link MatsFactory} on which outgoing messages will be sent, and on which the receiving
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
        int corePoolSize = Math.max(5, matsFactory.getFactoryConfig().getConcurrency() * 4);
        int maximumPoolSize = Math.max(100, matsFactory.getFactoryConfig().getConcurrency() * 20);
        return createMatsFuturizer(matsFactory, endpointIdPrefix, corePoolSize, maximumPoolSize, 50_000);
    }

    /**
     * Creates a MatsFuturizer, <b>and you should only need one per MatsFactory</b> (which again mostly means one per
     * application or micro-service or JVM). With this constructor you can specify the number of threads in the
     * future-completer-pool with the parameters "corePoolSize" and "maxPoolSize" threads, which effectively means min
     * and max. The pool is set up to let non-core threads expire after 5 minutes. You must also specify the max number
     * of outstanding promises, if you want no effective limit, use {@link Integer#MAX_VALUE}.
     *
     * @param matsFactory
     *            the underlying {@link MatsFactory} on which outgoing messages will be sent, and on which the receiving
     *            {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *            SubscriptionTerminator} will be created.
     * @param endpointIdPrefix
     *            the first part of the endpointId, which typically should be some "class-like" construct denoting the
     *            service name, like "OrderService" or "InventoryService".
     * @param corePoolSize
     *            the minimum number of threads in the future-completer-pool of threads.
     * @param maxPoolSize
     *            the maximum number of threads in the future-completer-pool of threads.
     * @param maxOutstandingPromises
     *            the maximum number of outstanding Promises before new are rejected. Should be a fairly high number,
     *            e.g. the default of {@link #createMatsFuturizer(MatsFactory, String)} is 50k.
     * @return the {@link MatsFuturizer}, which is tied to a newly created
     *         {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     *         SubscriptionTerminator}.
     */
    public static MatsFuturizer createMatsFuturizer(MatsFactory matsFactory, String endpointIdPrefix,
            int corePoolSize, int maxPoolSize, int maxOutstandingPromises) {
        return new MatsFuturizer(matsFactory, endpointIdPrefix, corePoolSize, maxPoolSize, maxOutstandingPromises);
    }

    protected final MatsFactory _matsFactory;
    protected final MatsInitiator _matsInitiator;
    protected final String _terminatorEndpointId;
    protected final ThreadPoolExecutor _futureCompleterThreadPool;
    protected final int _maxOutstandingPromises;
    protected final MatsEndpoint<Void, String> _replyHandlerEndpoint;

    protected MatsFuturizer(MatsFactory matsFactory, String endpointIdPrefix, int corePoolSize, int maxPoolSize,
            int maxOutstandingPromises) {
        _matsFactory = matsFactory;
        _matsInitiator = matsFactory.getDefaultInitiator();
        _terminatorEndpointId = endpointIdPrefix + ".private.Futurizer."
                + _matsFactory.getFactoryConfig().getNodename();
        _futureCompleterThreadPool = _newThreadPool(corePoolSize, maxPoolSize);
        _maxOutstandingPromises = maxOutstandingPromises;
        _replyHandlerEndpoint = _matsFactory.subscriptionTerminator(_terminatorEndpointId, String.class,
                MatsObject.class,
                this::_handleRepliesForPromises);
        _startTimeouterThread();
    }

    /**
     * An instance of this class will be the return value of any {@link CompletableFuture}s created with the
     * {@link MatsFuturizer}. It will contain the reply from the requested endpoint, and the
     * {@link DetachedProcessContext} from the received message, from where you can get any incoming
     * {@link DetachedProcessContext#getBytes(String) "sideloads"} and other metadata. It also contains a timestamp of
     * when the outgoing message was initiated.
     *
     * @param <T>
     *            the type of the reply class.
     */
    public static class Reply<T> {
        public final DetachedProcessContext context;
        public final T reply;
        public final long initiationTimestamp;

        public Reply(DetachedProcessContext context, T reply, long initiationTimestamp) {
            this.context = context;
            this.reply = reply;
            this.initiationTimestamp = initiationTimestamp;
        }

        public DetachedProcessContext getContext() {
            return context;
        }

        public T getReply() {
            return reply;
        }

        public long getInitiationTimestamp() {
            return initiationTimestamp;
        }
    }

    /**
     * This exception is raised through the {@link CompletableFuture} if the timeout specified when getting the
     * {@link CompletableFuture} is reached (to get yourself a future, use the
     * {@link #futurizeInteractiveUnreliable(String, String, String, Class, Object) futurizeXYZ(..)} methods). The
     * exception is passed to the waiter on the future by {@link CompletableFuture#completeExceptionally(Throwable)}.
     */
    public static class MatsFuturizerTimeoutException extends RuntimeException {
        private final long initiationTimestamp;
        private final String traceId;

        public MatsFuturizerTimeoutException(String message, long initiationTimestamp, String traceId) {
            super(message);
            this.initiationTimestamp = initiationTimestamp;
            this.traceId = traceId;
        }

        public long getInitiationTimestamp() {
            return initiationTimestamp;
        }

        public String getTraceId() {
            return traceId;
        }
    }

    /**
     * <b>NOTICE: This variant must <u>only</u> be used for "GET"-style requests where none of the endpoints the call
     * flow passes will add, remove or alter any state of the system</b> - This method initiates an
     * <b>{@link MatsInitiate#nonPersistent() non-persistent}</b> (unreliable), <b>{@link MatsInitiate#interactive()
     * interactive}</b> (prioritized) request-message to the specified endpoint, returning a {@link CompletableFuture}
     * that will be {@link CompletableFuture#complete(Object) completed} with the reply from the requested endpoint. The
     * internal MatsFuturizer timeout will be set to <b>2 minutes</b>, meaning that if there is no reply forthcoming
     * within that time, the {@link CompletableFuture} will be {@link CompletableFuture#completeExceptionally(Throwable)
     * completed exceptionally} with a {@link MatsFuturizerTimeoutException MatsFuturizerTimeoutException}, and the
     * Promise deleted from the futurizer. 2 minutes is probably too long to wait for any normal interaction with a
     * system, so if you use the {@link CompletableFuture#get(long, TimeUnit) CompletableFuture.get(timeout)} method of
     * the returned future, you might want to put a lower timeout there - if the answer hasn't come within that time,
     * you'll get a {@link TimeoutException}. If you instead use the non-param variant {@link CompletableFuture#get()
     * get()}, you will get an {@link ExecutionException} when the 2 minutes have passed (that exception's
     * {@link ExecutionException#getCause() cause} will be the {@link MatsFuturizerTimeoutException
     * MatsFuturizerTimeoutException} mentioned above).
     * <p/>
     * The goal of this method is to be able to get hold of e.g. account holdings, order statuses etc, for presentation
     * to a user. The thinking is that if such a flow fails where a message of the call flow disappears, this won't make
     * for anything else than a bit annoyed user: No important state change, like the adding, deleting or change of an
     * order, will be lost. Therefore, <i>non-persistent</i>. At the same time, to make the user super happy in the
     * ordinary circumstances, all messages in this call flow will be prioritized, and thus skip any queue backlogs that
     * have arose on any of the call flow's endpoints, e.g. due to some massive batch of (background) processes
     * executing at the same time. Therefore, <i>interactive</i>. Notice that with both of these features combined, you
     * get very fast messaging, as non-persistent means that the message will not have to be stored to permanent storage
     * at any point, while interactive means that it will skip any backlogged queues.
     *
     * @param traceId
     *            TraceId of the resulting Mats call flow, see {@link MatsInitiate#traceId(String)}
     * @param from
     *            the "from" of the initiation, see {@link MatsInitiate#from(String)}
     * @param to
     *            to which Mats endpoint the request should go, see {@link MatsInitiate#to(String)}
     * @param replyClass
     *            which expected reply DTO class that the requested endpoint replies with.
     * @param request
     *            the request DTO that should be sent to the endpoint, see {@link MatsInitiate#request(Object)}
     * @param <T>
     *            the type of the reply DTO.
     * @return a {@link CompletableFuture} which will be resolved with a {@link Reply}-instance that contains both some
     *         meta-data, and the {@link Reply#reply reply} from the requested endpoint.
     */
    public <T> CompletableFuture<Reply<T>> futurizeInteractiveUnreliable(String traceId, String from, String to,
            Class<T> replyClass, Object request) {
        return futurizeGeneric(traceId, from, to, 2, TimeUnit.MINUTES, replyClass, request,
                msg -> msg.interactive().nonPersistent());
    }

    /**
     * The generic form of initiating a request-message that returns a {@link CompletableFuture}, which enables you to
     * tailor all properties. To set interactive or nonPersistent-flags, or to tack on any
     * ({@link MatsInitiate#addBytes(String, byte[]) "sideloads"} to the outgoing message, use the "extraMessageInit"
     * parameter, which directly is the {@link InitiateLambda InitiateLambda} that the MatsFuturizer initiation is
     * using.
     * <p/>
     * For a bit more explanation, please read JavaDoc of
     * {@link #futurizeInteractiveUnreliable(String, String, String, Class, Object) futurizeInteractiveUnreliable(..)}
     *
     * @param traceId
     *            TraceId of the resulting Mats call flow, see {@link MatsInitiate#traceId(String)}
     * @param from
     *            the "from" of the initiation, see {@link MatsInitiate#from(String)}
     * @param to
     *            to which Mats endpoint the request should go, see {@link MatsInitiate#to(String)}
     * @param timeout
     *            how long before the internal timeout-mechanism of MatsFuturizer kicks in and the future is
     *            {@link CompletableFuture#completeExceptionally(Throwable) completed exceptionally} with a
     *            {@link MatsFuturizerTimeoutException}.
     * @param unit
     *            the unit of time of the 'timeout' parameter.
     * @param replyClass
     *            which expected reply DTO class that the requested endpoint replies with.
     * @param request
     *            the request DTO that should be sent to the endpoint, see {@link MatsInitiate#request(Object)}
     * @param extraMessageInit
     *            the {@link InitiateLambda} that the MatsFuturizer is employing to initiate the outgoing message, which
     *            you can use to tailor the message, e.g. setting the {@link MatsInitiate#interactive()
     *            interactive}-flag or tacking on {@link MatsInitiate#addBytes(String, byte[]) "sideloads"}.
     * @param <T>
     *            the type of the reply DTO.
     * @return a {@link CompletableFuture} which will be resolved with a {@link Reply}-instance that contains both some
     *         meta-data, and the {@link Reply#reply reply} from the requested endpoint.
     */
    public <T> CompletableFuture<Reply<T>> futurizeGeneric(String traceId, String from, String to,
            int timeout, TimeUnit unit, Class<T> replyClass, Object request, InitiateLambda extraMessageInit) {
        Promise<T> promise = _createPromise(traceId, from, replyClass, timeout, unit);
        _assertFuturizerRunning();
        _enqueuePromise(promise);
        _sendRequestToFulfillPromise(from, to, traceId, request, extraMessageInit, promise);
        return promise._future;
    }

    /**
     * @return the number of outstanding promises, not yet completed or timed out.
     */
    public int getOutstandingPromiseCount() {
        synchronized (_correlationIdToPromiseMap) {
            return _correlationIdToPromiseMap.size();
        }
    }

    /**
     * @return the future-completer-thread-pool, for introspection. If you mess with it, you <i>will</i> be sorry..!
     */
    public ThreadPoolExecutor getCompleterThreadPool() {
        return _futureCompleterThreadPool;
    }

    // ===== Internal classes and methods, can be overridden if you want to make a customized MatsFuturizer

    protected static class Promise<T> implements Comparable<Promise<?>> {
        public final String _traceId;
        public final String _correlationId;
        public final String _from;
        public final long _initiationTimestamp;
        public final long _timeoutTimestamp;
        public final Class<T> _replyClass;
        public final CompletableFuture<Reply<T>> _future;

        public Promise(String traceId, String correlationId, String from, long initiationTimestamp,
                long timeoutTimestamp, Class<T> replyClass, CompletableFuture<Reply<T>> future) {
            _traceId = traceId;
            _correlationId = correlationId;
            _from = from;
            _initiationTimestamp = initiationTimestamp;
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

    protected final AtomicInteger _threadNumber = new AtomicInteger();

    protected ThreadPoolExecutor _newThreadPool(int corePoolSize, int maximumPoolSize) {
        // Trick to make ThreadPoolExecutor work as anyone in the world would expect:
        // Have a constant pool of "corePoolSize", and then as more tasks are concurrently running than threads
        // available, you increase the number of threads until "maximumPoolSize", at which point the rest go on queue.

        // Snitched from https://stackoverflow.com/a/24493856

        // Part 1: So, we extend a LinkedTransferQueue to behave a bit special on "offer(..)":
        LinkedTransferQueue<Runnable> queue = new LinkedTransferQueue<Runnable>() {
            @Override
            public boolean offer(Runnable e) {
                // If there are any pool thread waiting for job, give it the job, otherwise return false.
                // The TPE interprets false as "no more room on queue", so it rejects it. (cont'd on part 2)
                return tryTransfer(e);
            }
        };
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                5L, TimeUnit.MINUTES, queue,
                r1 -> new Thread(r1, "MatsFuturizer completer #" + _threadNumber.getAndIncrement()));

        // Part 2: We make a special RejectionExecutionHandler ...
        threadPool.setRejectedExecutionHandler((r, executor) -> {
            // ... which upon rejection due to "full queue" puts the task on queue nevertheless (LTQ is not bounded).
            ((LinkedTransferQueue<Runnable>) executor.getQueue()).put(r);
        });

        return threadPool;
    }

    protected <T> Promise<T> _createPromise(String traceId, String from, Class<T> replyClass,
            int timeout, TimeUnit unit) {
        long timeoutMillis = unit.toMillis(timeout);
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("Timeout in milliseconds cannot be zero or negative [" + timeoutMillis
                    + "].");
        }
        String correlationId = RandomString.randomCorrelationId();
        long timestamp = System.currentTimeMillis();
        CompletableFuture<Reply<T>> future = new CompletableFuture<>();
        return new Promise<>(traceId, correlationId, from, timestamp, timestamp + timeoutMillis, replyClass, future);
    }

    protected <T> void _enqueuePromise(Promise<T> promise) {
        synchronized (_correlationIdToPromiseMap) {
            if (_correlationIdToPromiseMap.size() >= _maxOutstandingPromises) {
                throw new IllegalStateException("There are too many Promises outstanding, so cannot add more"
                        + " - limit is [" + _maxOutstandingPromises + "].");
            }
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

    protected void _handleRepliesForPromises(ProcessContext<Void> context, String correlationId,
            MatsObject matsObject) {
        // Immediately pick this out of the map & queue
        Promise<?> promise;
        synchronized (_correlationIdToPromiseMap) {
            // Find the Promise from the CorrelationId
            promise = _correlationIdToPromiseMap.remove(correlationId);
            // Did we find it?
            if (promise != null) {
                // -> Yes, found - remove it from the PriorityQueue too.
                _timeoutSortedPromises.remove(promise);
            }
            // NOTE: We don't bother pinging the Timeouter, as he'll find out himself soon enough if this was first.
        }
        // ?: Did we still have the Promise?
        if (promise == null) {
            // -> Promise gone, log on INFO and exit (it was logged on WARN when it was actually timed out).
            MDC.put("traceId", context.getTraceId());
            log.info(LOG_PREFIX + "Promise gone! Got reply from [" + context
                    .getFromStageId() + "] for Future with traceId:[" + context.getTraceId()
                    + "], but the Promise had timed out.");
            MDC.remove("traceId");
            return;
        }

        // ----- We have Promise, and shall now fulfill it. Send off to pool thread.

        _futureCompleterThreadPool.execute(() -> {
            try {
                MDC.put("traceId", promise._traceId);
                // NOTICE! We don't log here, as the SubscriptionTerminator already has logged the ordinary mats lines.
                log.debug(LOG_PREFIX + "Completing promise from [" + promise._from + "]: [" + promise + "]");

                Object replyObject;
                try {
                    replyObject = _deserializeReply(matsObject, promise._replyClass);
                }
                catch (IllegalArgumentException e) {
                    log.error("Got problems completing Future due to failing to deserialize the incoming object to"
                            + " expected class [" + promise._replyClass.getName() + "], thus doing"
                            + " future.completeExceptionally(..) with the [" + e.getClass().getSimpleName() + "]."
                            + " Initiated from [" + promise._from + "], with reply from [" + context.getFromStageId()
                            + "], traceId [" + context.getTraceId() + "]", e);
                    promise._future.completeExceptionally(e);
                    return;
                }
                _uncheckedComplete(context, replyObject, promise);
            }
            // NOTICE! This catch will probably never be triggered, as if .thenAccept() and similar throws,
            // the CompletableFuture evidently handles it and completes the future exceptionally.
            catch (Throwable t) {
                log.error(LOG_PREFIX + "Got problems completing Future initiated from [" + promise._from
                        + "], with reply from [" + context.getFromStageId()
                        + "], traceId:[" + context.getTraceId() + "]", t);
            }
            finally {
                MDC.remove("traceId");
            }
        });
    }

    protected Object _deserializeReply(MatsObject matsObject, Class<?> toClass) {
        return matsObject.toClass(toClass);
    }

    @SuppressWarnings("unchecked")
    protected void _uncheckedComplete(ProcessContext<Void> context, Object replyObject, Promise<?> promise) {
        Reply<?> tReply = new Reply<>(context, replyObject, promise._initiationTimestamp);
        promise._future.complete((Reply) tReply);
    }

    protected volatile boolean _runFlag = true;

    protected void _startTimeouterThread() {
        Runnable timeouter = () -> {
            log.info(LOG_PREFIX + "MatsFuturizer Timeouter-thread: Started!");
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
                                // E-> This is the Promise that is next in line to timeout.
                                _nextInLineToTimeout = peekPromise;
                                // This Promise has >0 milliseconds left before timeout, so calculate how long to sleep.
                                sleepMillis = peekPromise._timeoutTimestamp - now;
                            }
                            else {
                                // We have no Promise next in line to timeout.
                                _nextInLineToTimeout = null;
                                // Chill for a while, then just check to be on the safe side..
                                sleepMillis = 30_000;
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
                            log.error(LOG_PREFIX + "Got an unexpected Throwable in the promise-timeouter-thread."
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
                    String msg = "The Promise/Future timed out! It was initiated from:[" + promise._from
                            + "] with traceId:[" + promise._traceId + "]."
                            + " Initiation was [" + (System.currentTimeMillis()
                                    - promise._initiationTimestamp) + " ms] ago, and its specified"
                            + " timeout was:[" + (promise._timeoutTimestamp
                                    - promise._initiationTimestamp) + "].";
                    log.warn(LOG_PREFIX + msg);
                    MDC.remove("traceId");
                    _futureCompleterThreadPool.execute(() -> {
                        try {
                            MDC.put("traceId", promise._traceId);
                            _timeoutCompleteExceptionally(promise, msg);
                        }
                        // NOTICE! This catch will probably never be triggered, as if .thenAccept() and similar throws,
                        // the CompletableFuture evidently handles it and completes the future exceptionally.
                        catch (Throwable t) {
                            log.error(LOG_PREFIX + "Got problems timing out Promise/Future initiated from:["
                                    + promise._from + "] with traceId:[" + promise._traceId + "]", t);
                        }
                        finally {
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

    protected void _timeoutCompleteExceptionally(Promise<?> promise, String msg) {
        promise._future.completeExceptionally(new MatsFuturizerTimeoutException(
                msg, promise._initiationTimestamp, promise._traceId));
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
        _futureCompleterThreadPool.shutdown();
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
                MDC.put("traceId", promise._traceId);
                promise._future.cancel(true);
            }
            // NOTICE! This catch will probably never be triggered, as if .thenAccept() and similar throws,
            // the CompletableFuture evidently handles it and completes the future exceptionally.
            catch (Throwable t) {
                log.error(LOG_PREFIX + "Got problems cancelling (due to shutdown) Promise/Future initiated from:["
                        + promise._from + "] with traceId:[" + promise._traceId + "]", t);
            }
            finally {
                MDC.remove("traceId");
            }
        }
    }
}
