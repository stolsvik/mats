package com.stolsvik.mats.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.spring.MatsMapping.MatsMappings;

/**
 * A method annotated with this repeatable annotation directly becomes a
 * {@link MatsFactory#single(String, Class, Class, com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda) Mats Single-stage
 * Endpoint} or a
 * {@link MatsFactory#terminator(String, Class, Class, com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda) Mats
 * Terminator Endpoint}, depending on whether it returns a value, or is void.
 * <p>
 * For the Single-Stage endpoint (where the return type is set), one method parameter should be annotated with
 * {@link Dto @Dto}: When the endpoint is invoked, it will be set to the incoming (request) Data Transfer Object - and
 * the argument's type thus also specifies its expected deserialization class. The method's return type represent the
 * outgoing reply Data Transfer Object.
 * <p>
 * For the Terminator endpoint (where the return type is <code>void</code>), one method parameter should be annotated
 * with {@link Dto @Dto}: When the endpoint is invoked, it will be set to the incoming (typically reply - or just
 * "message") Data Transfer Object - and the argument's type thus also specifies its expected deserialization class. The
 * method's return type represent the outgoing reply Data Transfer Object. In addition, another method parameter can be
 * annotated with {@link Sto @Sto}, which will be the "State Transfer Object" - this is typically the object which an
 * initiator supplied to the {@link MatsInitiator#initiate(com.stolsvik.mats.MatsInitiator.InitiateLambda) initiate
 * call} when it set this Terminator endpoint as the {@link MatsInitiate#replyTo(String) replyTo} endpointId.
 * <p>
 * The annotation allows for some configuration...
 *
 * @author Endre Stølsvik - 2016-05-19 - http://endre.stolsvik.com
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Repeatable(MatsMappings.class)
public @interface MatsMapping {
    /**
     * The Mats <em>Endpoint Id</em> that this endpoint should listen to.
     *
     * @return the Mats <em>Endpoint Id</em> which this endpoint listens to.
     */
    String endpointId();

    @Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public static @interface MatsMappings {
        MatsMapping[]value();
    }
}
