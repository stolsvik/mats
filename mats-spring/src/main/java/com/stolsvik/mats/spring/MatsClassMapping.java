package com.stolsvik.mats.spring;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Service;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.MatsClassMapping.MatsClassMappings;

/**
 * This is pure magic.
 *
 * @author Endre Stølsvik 2019-08-17 21:53 - http://stolsvik.com/, endre@stolsvik.com
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.ANNOTATION_TYPE })
@Repeatable(MatsClassMappings.class)
@Service
public @interface MatsClassMapping {
    /**
     * The Mats <em>Endpoint Id</em> that this endpoint should listen to.
     *
     * @return the Mats <em>Endpoint Id</em> which this endpoint listens to.
     */
    @AliasFor("value")
    String endpointId() default "";

    /**
     * Alias for "endpointId", so that if you only need to set the endpointId, you can do so directly:
     * <code>@MatsEndpointSetup("endpointId")</code>
     *
     * @return the endpointId.
     */
    @AliasFor("endpointId")
    String value() default "";

    /**
     * Specifies the {@link MatsFactory} to use by means of a specific qualifier annotation type (which thus must be
     * meta-annotated with {@link Qualifier}). Notice that this will search for the custom qualifier annotation
     * <i>type</i>, as opposed to if you add the annotation to the @MatsEndpointSetup-annotated method directly, in
     * which case it "equals" the annotation <i>instance</i> (as Spring also does when performing injection with such
     * qualifiers). The difference comes into play if the annotation has values, where e.g. a
     * <code>@SpecialMatsFactory(location="central")</code> is not equal to
     * <code>@SpecialMatsFactory(location="region_west")</code> - but they are equal when comparing types, as the
     * qualification here does. Thus, if using this qualifier-approach, you should probably not use values on your
     * custom qualifier annotations (instead make separate custom qualifier annotations, e.g.
     * <code>@MatsFactoryCentral</code> and <code>@MatsFactoryRegionWest</code> for the example).
     *
     * @return the <i>custom qualifier type</i> which the wanted {@link MatsFactory} is qualified with.
     */
    Class<? extends Annotation> matsFactoryCustomQualifierType() default Annotation.class;

    /**
     * Specified the {@link MatsFactory} to use by means of specifying the <code>@Qualifier</code> <i>value</i>. Spring
     * performs such lookup by first looking for actual qualifiers with the specified value, e.g.
     * <code>@Qualifier(value="the_value")</code>. If this does not produce a result, it will try to find a bean with
     * this value as the bean name.
     *
     * @return the <i>qualifier value</i> which the wanted {@link MatsFactory} is qualified with.
     */
    String matsFactoryQualifierValue() default "";

    /**
     * Specified the {@link MatsFactory} to use by means of specifying the bean name of the {@link MatsFactory}.
     *
     * @return the <i>bean name</i> of the wanted {@link MatsFactory}.
     */
    String matsFactoryBeanName() default "";

    /**
     * Each method in the class that shall correspond to a Stage on the Mats endpoint must be annotated with this
     * <code>@Stage</code> annotation. An {@link #ordinal() ordinal} must be assigned to each stage, so that Mats knows
     * which order the stages are in - read more about the ordinal number at {@link #ordinal()}. The initial stage must
     * have the ordinal zero, which also shall be the first stage in the resulting sorted list of Stages (i.e. negative
     * values are not allowed) - this is the Stage which gets incoming messages targeted at the
     * {@link MatsClassMapping#endpointId() endpointId} of this endpoint.
     */
    @Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface Stage {
        /**
         * Constant for 0 (zero), the initial Stage's ordinal.
         */
        int INITIAL = 0;

        /**
         * The ordinal of this Stage in the sequence of stages of this endpoint - that is, an integer that expresses the
         * relative position of this Stage wrt. to the other stages. The initial Stage must have the ordinal zero, you
         * may use the constant {@link #INITIAL}. The magnitude of the number does not matter, only the "sort order", so
         * 0, 1, 2 is just as good as 0, 3, 5, which is just as good as 0, 4527890, 4527990 - although one can
         * definitely discuss the relative merits between each approach. An idea is the cool'n'retro Commodore
         * BASIC-style of line numbers, which commonly was to use values in multiple of 10, i.e. 0 (for the Initial),
         * then 10, 20, 30. The rationale is that you then quickly can add a line between 10 and 20 by sticking in a 15
         * there.
         *
         * @return the ordinal of this Stage in the sequence of stages of this endpoint.
         */
        @AliasFor("value")
        int ordinal() default -1;

        /**
         * Alias for "ordinal", so that if you only need to set the ordinal (which relative position in the sequence of
         * stages this Stage is), you can do so directly: <code>@Stage(15)</code>.
         * 
         * @see #ordinal()
         *
         * @return the ordinal of this Stage in the sequence of stages of this endpoint.
         */
        @AliasFor("ordinal")
        int value() default -1;

    }

    @Target({ ElementType.TYPE, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface MatsClassMappings {
        MatsClassMapping[] value();
    }
}
