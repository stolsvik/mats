package com.stolsvik.mats;

import com.stolsvik.mats.MatsConfig.StartClosable;

/**
 * A representation of a process stage of a {@link MatsEndpoint}. Either constructed implicitly (for single-stage
 * endpoints, and terminators), or by invoking the
 * {@link MatsEndpoint#stage(Class, com.stolsvik.mats.MatsEndpoint.ProcessLambda) MatsEndpoint.stage(...)}-methods
 * 
 * @author Endre Stølsvik - 2015-07-11 - http://endre.stolsvik.com
 */
public interface MatsStage extends StartClosable {

    /**
     * @return the {@link StageConfig} for this stage.
     */
    StageConfig getStageConfig();

    /**
     * Starts this stage, thereby firing up the queue processing using a set of threads, the number decided by the
     * {@link StageConfig#getConcurrency()} for each stage. Note that a
     * {@link MatsFactory.FactoryConfig#setStartDelay(int) time delay} maybe be imposed, to account for application
     * startup asynchronicity (e.g. not all services having their startup finished yet).
     * <p>
     * Will generally be invoked implicitly by {@link MatsEndpoint#start()}. The only reason for calling this should be
     * if its corresponding {@link #close()} method has been invoked to stop processing.
     * <p>
     * If the {@link MatsFactory} is stopped ("closed") when this method is invoked, it will not start until the factory
     * is started.
     */
    @Override
    void start();

    /**
     * Stops this stage. This may be used to temporarily stop processing of this stage by means of some external
     * monitor/inspecting mechanism (e.g. in cases where it has been showed to produce results that breaks downstream
     * stages or endpoints, or itself produces <i>Dead Letter Queue</i>-entries due to some external problem). It is
     * possible to {@link #start()} the stage again.
     */
    @Override
    void close();

    /**
     * Provides for both configuring the stage (before it is started), and introspecting the configuration.
     */
    interface StageConfig extends MatsConfig {
        /**
         * @return the class expected for incoming messages to this process stage.
         */
        Class<?> getIncomingMessageClass();
    }
}