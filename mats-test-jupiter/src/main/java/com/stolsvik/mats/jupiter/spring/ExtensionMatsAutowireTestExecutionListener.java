package com.stolsvik.mats.jupiter.spring;

import java.lang.reflect.Field;
import java.util.Set;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.test.context.TestContext;

import com.stolsvik.mats.test.abstractunit.AbstractMatsTestExecutionListener;

/**
 * Use this Test Execution Listener to autowire Jupiter Extensions, i.e. so that any fields in the Extension annotated
 * with @Inject will be autowired (It will also take fields with @Autowire, but we use @Inject for own code).
 * <p>
 * Annotate test class with the following annotation (typically in your project's super-class for tests).
 * <p>
 * Using shortened name so that it fits on one line in the actual code using it.
 * <pre>
 * {@literal @}TestExecutionListeners(value = ExtensionMatsAutowireTestExecutionListener.class, mergeMode = MergeMode.MERGE_WITH_DEFAULTS)
 * </pre>
 *
 * @author Kevin Mc Tiernan, 2020-11-03, kmctiernan@gmail.com
 */
public class ExtensionMatsAutowireTestExecutionListener extends AbstractMatsTestExecutionListener {

    /**
     * Performs dependency injection on {@link RegisterExtension @RegisterExtension} fields in test-class as supplied by
     * testContext.
     */
    @Override
    public void prepareTestInstance(TestContext testContext) throws Exception {
        AutowireCapableBeanFactory beanFactory = testContext.getApplicationContext().getAutowireCapableBeanFactory();

        // Get all fields in test class annotated with @RegisterExtension
        Set<Field> testExtensionFields = findFields(testContext.getTestClass(), RegisterExtension.class);

        // Use bean factory to autowire all extensions
        for (Field testField : testExtensionFields) {
            Object ruleObject = testField.get(testContext.getTestInstance());
            beanFactory.autowireBean(ruleObject);
        }
    }
}
