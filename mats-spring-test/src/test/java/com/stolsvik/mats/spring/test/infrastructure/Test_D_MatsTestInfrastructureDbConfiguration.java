package com.stolsvik.mats.spring.test.infrastructure;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.MatsFactoryWrapper;
import com.stolsvik.mats.MatsFactory.MatsWrapper;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.spring.test.MatsTestInfrastructureConfiguration;
import com.stolsvik.mats.spring.test.MatsTestInfrastructureDbConfiguration;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.TestH2DataSource;
import com.stolsvik.mats.util.MatsFuturizer;

/**
 * Tests that if we make a {@link MatsSerializer} in the Spring Context in the test, the
 * {@link MatsTestInfrastructureConfiguration} will pick it up
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = { MatsTestInfrastructureDbConfiguration.class })
public class Test_D_MatsTestInfrastructureDbConfiguration {

    // The Mats Test Infrastructure, with DataSource

    @Inject
    private MatsFactory _matsFactory;

    @Inject
    private MatsInitiator _matsInitiator;

    @Inject
    private MatsFuturizer _matsFuturizer;

    @Inject
    private MatsTestLatch _matsTestLatch;

    @Inject
    private TestH2DataSource _dataSource;

    @Test
    public void assertMatsInfrastructureInjected() {
        Assert.assertNotNull("MatsFactory should be in Spring context", _matsFactory);
        Assert.assertNotNull("MatsInitiator should be in Spring context", _matsInitiator);
        Assert.assertNotNull("MatsFuturizer should be in Spring context", _matsFuturizer);
        Assert.assertNotNull("MatsTestLatch should be in Spring context", _matsTestLatch);
    }

    @Test
    public void assertDataSourceInjected() {
        Assert.assertNotNull("DataSource should be in Spring context", _dataSource);
    }

    @Test
    public void assert_Same_DataSource_was_used_to_make_MatsFactory() {
        // NOTE: The injected MatsFactory will be wrapped by TestSpringMatsFactoryProvider.

        Common.assertSameDataSourceInMatsFactory(_matsFactory, _dataSource);
    }
}
