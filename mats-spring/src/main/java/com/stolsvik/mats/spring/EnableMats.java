package com.stolsvik.mats.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

import com.stolsvik.mats.spring.MatsSpringAnnotationBeanPostProcessor.MatsSpringConfiguration;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(MatsSpringConfiguration.class)
@Documented
public @interface EnableMats {

}
