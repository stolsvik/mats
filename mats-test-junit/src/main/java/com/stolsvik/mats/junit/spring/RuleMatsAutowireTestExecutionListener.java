package com.stolsvik.mats.junit.spring;

import java.lang.reflect.Field;
import java.util.Set;

import org.junit.Rule;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.test.context.TestContext;

import com.stolsvik.mats.test.abstractunit.AbstractMatsTestExecutionListener;

/**
 * Use this Test Execution Listener to autowire JUnit Rules, i.e. so that any fields in the Rule annotated with @Inject
 * will be autowired (It will also take fields with @Autowire, but we use @Inject for own code).
 * <p>
 * Annotate test class with the following annotation (typically in your project's super-class for tests).
 * <p>
 * Using shortened name so that it fits on one line in the actual code using it.
 * <pre>
 * {@literal @}TestExecutionListeners(value = RuleMatsAutowireTestExecutionListener.class, mergeMode = MergeMode.MERGE_WITH_DEFAULTS)
 * </pre>
 *
 * @author Kevin Mc Tiernan, 2020-10-22, kmctiernan@gmail.com
 */
public class RuleMatsAutowireTestExecutionListener extends AbstractMatsTestExecutionListener {

    /**
     * Performs dependency injection on {@link Rule @Rule} fields in test-class as supplied by testContext.
     */
    @Override
    public void prepareTestInstance(TestContext testContext) throws Exception {
        AutowireCapableBeanFactory beanFactory = testContext.getApplicationContext().getAutowireCapableBeanFactory();

        // Get all fields in test class annotated with @Rule
        Set<Field> testRuleFields = findFields(testContext.getTestClass(), Rule.class);

        // Use bean factory to autowire all rules
        for (Field testField : testRuleFields) {
            Object ruleObject = testField.get(testContext.getTestInstance());
            beanFactory.autowireBean(ruleObject);
        }
    }
}
