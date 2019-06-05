package com.stolsvik.mats.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.stolsvik.mats.MatsFactory;

/**
 * Enables Mats "SpringConfig", which is bean-scanning for methods on Spring beans annotated with {@link MatsMapping}
 * and {@link MatsStaged}, conceptually inspired by the {@literal @EnableWebMvc} annotation. One (or several)
 * {@link MatsFactory}s must be set up in the Spring context. Methods having the specified annotations will get Mats
 * endpoints set up for them on the <code>MatsFactory</code>.
 * <p>
 * This annotation simply imports the {@link MatsSpringConfiguration} class, which is a Spring
 * {@link Configuration @Configuration}. Read more JavaDoc there!
 *
 * @author Endre Stølsvik - 2016-05-21 - http://endre.stolsvik.com
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(MatsSpringConfiguration.class)
@Documented
public @interface EnableMats {

}
