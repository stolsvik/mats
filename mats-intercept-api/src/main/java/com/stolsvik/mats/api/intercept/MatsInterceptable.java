package com.stolsvik.mats.api.intercept;

import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor.MatsInitiateInterceptorProvider;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.MatsStageInterceptorProvider;

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

    void removeInitiationInterceptorSingleton(MatsInitiateInterceptor initiateInterceptor);

    // ===== Stage

    void addStageInterceptorProvider(MatsStageInterceptorProvider stageInterceptorProvider);

    void removeStageInterceptorProvider(MatsStageInterceptorProvider stageInterceptorProvider);

    void addStageInterceptorSingleton(MatsStageInterceptor stageInterceptor);

    void removeStageInterceptorSingleton(MatsStageInterceptor stageInterceptor);
}
