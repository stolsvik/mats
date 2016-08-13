package com.stolsvik.mats.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.stolsvik.mats.spring.MatsStaged.MatsStageds;

/**
 * A method annotated with this repeatable annotation specifies a method that shall <em>set up</em> a (usually)
 * Multi-Staged Mats Endpoint. Note that as opposed to {@link MatsMapping @MatsMapping}, this method will be invoked
 * <em>once</em> to <i>set up</i> the endpoint, and will not be invoked each time when the Mats endpoint is invoked (as
 * is the case with {@literal @MatsMapping}).
 *
 * @author Endre Stølsvik - 2016-08-07 - http://endre.stolsvik.com
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Repeatable(MatsStageds.class)
public @interface MatsStaged {
    /**
     * The Mats <em>Endpoint Id</em> that this endpoint should listen to.
     *
     * @return the Mats <em>Endpoint Id</em> which this endpoint listens to.
     */
    String endpointId();

    /**
     * The Mats <em>State Transfer Object</em> class that should be employed for all of the stages for this endpoint.
     *
     * @return the <em>State Transfer Object</em> class that should be employed for all of the stages for this endpoint.
     */
    Class<?>state() default Void.class;

    /**
     * The Mats <em>Data Transfer Object</em> class that should be employed for all of the stages for this endpoint.
     *
     * @return the <em>Data Transfer Object</em> class that will be returned by the last stage of the staged endpoint.
     */
    Class<?>reply() default Void.class;

    @Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public static @interface MatsStageds {
        MatsStaged[]value();
    }
}
