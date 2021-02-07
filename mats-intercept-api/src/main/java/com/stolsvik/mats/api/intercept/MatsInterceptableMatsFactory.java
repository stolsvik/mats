package com.stolsvik.mats.api.intercept;

import com.stolsvik.mats.MatsFactory;

/**
 * Combines the interfaces {@link MatsInterceptable} and {@link MatsFactory}.
 *
 * @author Endre Stølsvik - 2021-02-07 - http://endre.stolsvik.com
 */
public interface MatsInterceptableMatsFactory extends MatsFactory, MatsInterceptable {
}
