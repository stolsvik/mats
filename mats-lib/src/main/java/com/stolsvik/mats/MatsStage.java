package com.stolsvik.mats;

import com.stolsvik.mats.MatsConfig.StartStoppable;

/**
 * A representation of a process stage of a {@link MatsEndpoint}. Either constructed implicitly (for single-stage
 * endpoints, and terminators), or by invoking the
 * {@link MatsEndpoint#stage(Class, com.stolsvik.mats.MatsEndpoint.ProcessLambda) MatsEndpoint.stage(...)}-methods on
 * {@link MatsFactory#staged(String, Class, Class) multi-stage} endpoints.
 *
 * @author Endre Stølsvik - 2015-07-11 - http://endre.stolsvik.com
 */
public interface MatsStage<I, S, R> extends StartStoppable {

    /**
     * @return the {@link StageConfig} for this stage.
     */
    StageConfig<I, S, R> getStageConfig();

    /**
     * Starts this stage, thereby firing up the queue processing using a set of threads, the number decided by the
     * {@link StageConfig#getConcurrency()} for each stage.
     * <p>
     * Will generally be invoked implicitly by {@link MatsEndpoint#start()}. The only reason for calling this should be
     * if its corresponding {@link #stop()} method has been invoked to stop processing.
     * <p>
     * If the parent {@link MatsFactory} is stopped when this method is invoked, this endpoint will not start until the
     * factory is started.
     */
    @Override
    void start();

    /**
     * Will wait until at least one processor of the stage has started and has entered the receive loop.
     */
    @Override
    void waitForStarted();

    /**
     * Stops this stage. This may be used to temporarily stop processing of this stage by means of some external
     * monitor/inspecting mechanism (e.g. in cases where it has been showed to produce results that breaks downstream
     * stages or endpoints, or itself produces <i>Dead Letter Queue</i>-entries due to some external problem). It is
     * possible to {@link #start()} the stage again.
     */
    @Override
    void stop();

    /**
     * Provides for both configuring the stage (before it is started), and introspecting the configuration.
     */
    interface StageConfig<I, S, R> extends MatsConfig {
        /**
         * @return the class expected for incoming messages to this process stage.
         */
        Class<I> getIncomingMessageClass();

        /**
         * @return the currently number of running Stage Processors (the actual concurrency - this might be different
         *         from {@link #getConcurrency} if the concurrency was set when stage was running.
         */
        int getRunningStageProcessors();
    }
}