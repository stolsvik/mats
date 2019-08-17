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
     * The method representing the initial Stage of the endpoint - the one that receives the message when a message is
     * sent to {@link MatsClassMapping#endpointId()} - must be annotated with this annotation.
     */
    @Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface Initial {

    }

    /**
     * Annotation that needs to be on all the endpoint's Stages, except the initial stage, which shall be annotated
     * with @{@link Initial}.
     */
    @Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface Stage {
        /**
         * The ordinal of this Stage in the sequence of stages of this endpoint - that is, an integer that expresses the
         * relative position of this Stage wrt. to the other stages. The magnitude of the number does not matter, only
         * the "sort order", so 10, 20, 30 is just as good as 1, 2, 3, which is just as good as 7, 4527890 and 4527990.
         * An idea is the cool'n'retro Commodore BASIC-style of line numbers, which commonly was "tens". The rationale
         * is that you then quickly can add a line between 10 and 20 by sticking in a 15 there.
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
        @AliasFor("endpointId")
        int value() default -1;

    }

    @Target({ ElementType.TYPE, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface MatsClassMappings {
        MatsClassMapping[] value();
    }
}
