package com.stolsvik.mats.api.intercept;

import java.util.Optional;

import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor.InitiateInterceptContext;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.StageInterceptContext;

/**
 * Specifies methods that an interceptable MatsFactory needs.
 *
 * @author Endre Stølsvik - 2021-02-07 - http://endre.stolsvik.com
 */
public interface MatsInterceptable {

    // ===== Initiation

    void addInitiationInterceptorProvider(MatsInitiateInterceptorProvider initiationInterceptorProvider);

    void removeInitiationInterceptorProvider(MatsInitiateInterceptorProvider initiationInterceptorProvider);

    void addInitiationInterceptorSingleton(MatsInitiateInterceptor initiateInterceptor);

    <T extends MatsInitiateInterceptor> Optional<T> getInitiationInterceptorSingleton(Class<T> interceptorClass);

    void removeInitiationInterceptorSingleton(MatsInitiateInterceptor initiateInterceptor);

    @FunctionalInterface
    interface MatsInitiateInterceptorProvider {
        /**
         * @param initiateInterceptContext
         *            the context of this initiation
         * @return a {@link MatsInitiateInterceptor} if you want to intercept this, or <code>null</code> if you do not.
         */
        MatsInitiateInterceptor provide(InitiateInterceptContext initiateInterceptContext);
    }

    // ===== Stage

    void addStageInterceptorProvider(MatsStageInterceptorProvider stageInterceptorProvider);

    void removeStageInterceptorProvider(MatsStageInterceptorProvider stageInterceptorProvider);

    void addStageInterceptorSingleton(MatsStageInterceptor stageInterceptor);

    <T extends MatsStageInterceptor> Optional<T> getStageInterceptorSingleton(Class<T> interceptorClass);

    void removeStageInterceptorSingleton(MatsStageInterceptor stageInterceptor);

    @FunctionalInterface
    interface MatsStageInterceptorProvider {
        /**
         * @param stageInterceptContext
         *            the context of this stage
         * @return a {@link MatsStageInterceptor} if you want to intercept this, or <code>null</code> if you do not.
         */
        MatsStageInterceptor provide(StageInterceptContext stageInterceptContext);
    }
}
