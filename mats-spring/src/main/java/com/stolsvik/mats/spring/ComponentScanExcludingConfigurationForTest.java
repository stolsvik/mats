package com.stolsvik.mats.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.AnnotationScopeMetadataResolver;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.ScopeMetadataResolver;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.annotation.AliasFor;

/**
 * A convenience @ComponentScan-meta-annotated which excludes any configuration classes using the special
 * {@link ConfigurationForTest @ConfigurationForTest} annotation instead of the standard <code>@Configuration</code>
 * annotation. By employing the {@link ConfigurationForTest} for tests' configuration classes (typically static inner),
 * and this annotation as the application's component scan, you avoid the problem whereby when running tests, a
 * component scan of the base package of the application will also include any configuration classes from the tests
 * (assuming that they, as customary is, resides in the same package as the application, but in "src/test/java". Thus,
 * this is not a problem when running the application normally, only when running any tests that include the
 * application's spring context setup that does a component scan).
 * <p>
 * Notice: All the properties of the @ComponentScan as of Spring 4.x is included using @AliasFor. However, if you want
 * need something from a future @ComponentScan which is not included here, you can just use the standard @ComponentScan,
 * making sure you include this excludeFilters property:
 * 
 * <pre>
 *  &#64;ComponentScan(excludeFilters = {
 *      &#64;Filter(type = FilterType.ANNOTATION, value = ConfigurationForTest.class)
 *  })
 * </pre>
 * 
 *
 * @see ConfigurationForTest
 * @see <a href="https://stackoverflow.com/a/51194221/39334">Stackoverflow 1</a>
 * @see <a href="https://stackoverflow.com/a/46344822/39334">Stackoverflow 2</a>
 *
 * @author Endre Stølsvik 2019-08-12 22:33 - http://stolsvik.com/, endre@stolsvik.com
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Configuration
// meta-annotation:
@ComponentScan(excludeFilters = {
        @Filter(type = FilterType.ANNOTATION, value = ConfigurationForTest.class)
})
public @interface ComponentScanExcludingConfigurationForTest {
    /**
     * Alias for {@link #basePackages}.
     * <p>
     * Allows for more concise annotation declarations if no other attributes are needed &mdash; for example,
     * {@code @ComponentScan("org.my.pkg")} instead of {@code @ComponentScan(basePackages = "org.my.pkg")}.
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "value")
    String[] value() default {};

    /**
     * Base packages to scan for annotated components.
     * <p>
     * {@link #value} is an alias for (and mutually exclusive with) this attribute.
     * <p>
     * Use {@link #basePackageClasses} for a type-safe alternative to String-based package names.
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "basePackages")
    String[] basePackages() default {};

    /**
     * Type-safe alternative to {@link #basePackages} for specifying the packages to scan for annotated components. The
     * package of each class specified will be scanned.
     * <p>
     * Consider creating a special no-op marker class or interface in each package that serves no purpose other than
     * being referenced by this attribute.
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "basePackageClasses")
    Class<?>[] basePackageClasses() default {};

    /**
     * The {@link BeanNameGenerator} class to be used for naming detected components within the Spring container.
     * <p>
     * The default value of the {@link BeanNameGenerator} interface itself indicates that the scanner used to process
     * this {@code @ComponentScan} annotation should use its inherited bean name generator, e.g. the default
     * {@link AnnotationBeanNameGenerator} or any custom instance supplied to the application context at bootstrap time.
     * 
     * @see AnnotationConfigApplicationContext#setBeanNameGenerator(BeanNameGenerator)
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "nameGenerator")
    Class<? extends BeanNameGenerator> nameGenerator() default BeanNameGenerator.class;

    /**
     * The {@link ScopeMetadataResolver} to be used for resolving the scope of detected components.
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "scopeResolver")
    Class<? extends ScopeMetadataResolver> scopeResolver() default AnnotationScopeMetadataResolver.class;

    /**
     * Indicates whether proxies should be generated for detected components, which may be necessary when using scopes
     * in a proxy-style fashion.
     * <p>
     * The default is defer to the default behavior of the component scanner used to execute the actual scan.
     * <p>
     * Note that setting this attribute overrides any value set for {@link #scopeResolver}.
     * 
     * @see ClassPathBeanDefinitionScanner#setScopedProxyMode(ScopedProxyMode)
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "scopedProxy")
    ScopedProxyMode scopedProxy() default ScopedProxyMode.DEFAULT;

    /**
     * Controls the class files eligible for component detection.
     * <p>
     * Consider use of {@link #includeFilters} and {@link #excludeFilters} for a more flexible approach.
     */
    // Notice: "ClassPathScanningCandidateComponentProvider.DEFAULT_RESOURCE_PATTERN" is package-private
    @AliasFor(annotation = ComponentScan.class, attribute = "resourcePattern")
    String resourcePattern() default "**/*.class";

    /**
     * Indicates whether automatic detection of classes annotated with {@code @Component} {@code @Repository},
     * {@code @Service}, or {@code @Controller} should be enabled.
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "useDefaultFilters")
    boolean useDefaultFilters() default true;

    /**
     * Specifies which types are eligible for component scanning.
     * <p>
     * Further narrows the set of candidate components from everything in {@link #basePackages} to everything in the
     * base packages that matches the given filter or filters.
     * <p>
     * Note that these filters will be applied in addition to the default filters, if specified. Any type under the
     * specified base packages which matches a given filter will be included, even if it does not match the default
     * filters (i.e. is not annotated with {@code @Component}).
     * 
     * @see #resourcePattern()
     * @see #useDefaultFilters()
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "includeFilters")
    Filter[] includeFilters() default {};

    /**
     * Specifies which types are not eligible for component scanning. <B>NOTICE: For this special variant, the default
     * is set to exclude @ConfigurationForTest annotated classes</B>
     * 
     * @see #resourcePattern
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "excludeFilters")
    // NOTICE: Must set the exclusion here as default, as that is what really what is applied - the stuff in the meta-
    // annotation at top of @interface definition is just to point out what happens (If this @AliasFor wasn't present,
    // it would pick up the one up there, though).
    Filter[] excludeFilters() default {
            @Filter(type = FilterType.ANNOTATION, value = ConfigurationForTest.class)
    };

    /**
     * Specify whether scanned beans should be registered for lazy initialization.
     * <p>
     * Default is {@code false}; switch this to {@code true} when desired.
     * 
     * @since 4.1
     */
    @AliasFor(annotation = ComponentScan.class, attribute = "lazyInit")
    boolean lazyInit() default false;

}
