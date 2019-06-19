package com.stolsvik.mats.spring.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.test.context.ActiveProfiles;

import com.stolsvik.mats.spring.jms.factories.MatsProfiles;

/**
 * The only thing this annotation does, is to meta-annotate the test class with
 * <code>@ActiveProfiles({@link MatsProfiles#PROFILE_MATS_TEST})</code>. You may just as well do this yourself, but this is
 * a few letter shorter, and slightly more concise.
 * 
 * @author Endre Stølsvik 2019-06-17 19:06 - http://stolsvik.com/, endre@stolsvik.com
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ActiveProfiles(MatsProfiles.PROFILE_MATS_TEST)
@Documented
public @interface MatsTestProfile {}
