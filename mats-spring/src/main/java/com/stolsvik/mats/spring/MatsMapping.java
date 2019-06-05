package com.stolsvik.mats.spring;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.spring.MatsMapping.MatsMappings;

/**
 * A method annotated with this repeatable annotation directly becomes a
 * {@link MatsFactory#single(String, Class, Class, com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda) Mats Single-stage
 * Endpoint} or a
 * {@link MatsFactory#terminator(String, Class, Class, com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda) Mats
 * Terminator Endpoint}, depending on whether the method specifies a return type, or is void.
 * <p>
 * For the Single-Stage endpoint (where the return type is set), one method parameter should be annotated with
 * {@link Dto @Dto}: When the endpoint is invoked, it will be set to the incoming (request) Data Transfer Object - and
 * the argument's type thus also specifies its expected deserialization class. The method's return type represent the
 * outgoing reply Data Transfer Object.
 * <p>
 * For the Terminator endpoint (where the return type is <code>void</code>), one method parameter should be annotated
 * with {@link Dto @Dto}: When the endpoint is invoked, it will be set to the incoming (typically request - or just
 * "message") Data Transfer Object - and the argument's type thus also specifies its expected deserialization class. The
 * method's return type represent the outgoing reply Data Transfer Object. In addition, another method parameter can be
 * annotated with {@link Sto @Sto}, which will be the "State Transfer Object" - this is typically the object which an
 * initiator supplied to the {@link MatsInitiator#initiate(com.stolsvik.mats.MatsInitiator.InitiateLambda) initiate
 * call} when it set this Terminator endpoint as the {@link MatsInitiate#replyTo(String, Object) replyTo} endpointId.
 * <p>
 * If you have a setup with multiple {@link MatsFactory}s, you must either have one (and only one) of the factories
 * denoted as {@link Primary @Primary}, or you must qualify which MatsFactory to use. This can be done by the following
 * means:
 * <ul>
 * <li>Add a {@link Qualifier @Qualifier(qualifiervalue)} annotation to the @MatsMapping-annotated method - this both
 * matches a MatsFactory with the same <code>@Qualifier</code>-annotation, and a MatsFactory whose bean name is the
 * 'qualifierValue' (this is the Spring default logic).
 * <li>Add a custom qualifier annotation (e.g. <code>@SpecialMatsFactory</code> or potentially
 * <code>@SpecialMatsFactory(location="somevalue")</code>
 * <li>Use the {@link #matsFactoryBeanName()} annotation value
 * <li>Use the {@link #matsFactoryQualifierValue()} annotation value
 * <li>Use the {@link #matsFactoryCustomQualifierType()} annotation value (please read the JavaDoc for special
 * considerations with this).
 * </ul>
 * You cannot specify more than one qualification per @MatsMapping. As mentioned, @MatsMapping is repeatable, so you can
 * specify multiple mappings on one method. However, the two first ways to qualify which MatsFactory to use (that is, by
 * means of annotating the @MatsMapping-annotated method with the qualifier annotation) will then apply to all of the
 * mappings, while the three annotation-value based qualifications applies to the specific mapping.
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

    /**
     * Specifies the {@link MatsFactory} to use by means of a specific qualifier annotation type (which thus must be
     * meta-annotated with {@link Qualifier}). Notice that this will search for the custom qualifier annotation
     * <i>type</i>, as opposed to if you add the annotation to the @MatsMapped-annotated method directly, in which case
     * it "equals" the annotation <i>instance</i> (as Spring also does when performing injection with such qualifiers).
     * The difference comes into play if the annotation has values, where e.g. a
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

    @Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface MatsMappings {
        MatsMapping[] value();
    }
}
