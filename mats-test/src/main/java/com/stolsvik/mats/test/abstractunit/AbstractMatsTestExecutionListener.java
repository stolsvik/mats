package com.stolsvik.mats.test.abstractunit;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.springframework.test.context.support.AbstractTestExecutionListener;

/**
 * Simple abstract class designed to hold one common method between the test execution listeners of junit and jupiter.
 *
 * @author Kevin Mc Tiernan, 2020-11-03, kmctiernan@gmail.com
 */
public abstract class AbstractMatsTestExecutionListener extends AbstractTestExecutionListener {

    /**
     * Find all fields in class with given annotation. Copied from StackOverflow, inspired by Apache Commons Lang.
     * <p>
     * https://stackoverflow.com/a/29766135
     */
    protected static Set<Field> findFields(Class<?> clazz, Class<? extends Annotation> annotation) {
        Set<Field> set = new HashSet<>();
        Class<?> c = clazz;
        while (c != null) {
            for (Field field : c.getDeclaredFields()) {
                if (field.isAnnotationPresent(annotation)) {
                    set.add(field);
                }
            }
            c = c.getSuperclass();
        }
        return set;
    }
}
