package com.stolsvik.mats.spring;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.config.AbstractFactoryBean;

import com.stolsvik.mats.MatsFactory;

/**
 * The intention of this test is to verify that a {@link MatsFactory} created through the use of a
 * {@link AbstractFactoryBean} is shutdown BEFORE the {@link javax.jms.ConnectionFactory JMS ConnectionFactory}.
 * The reason we want to verify that this is true is to ensure that there are no unwanted exceptions being generated
 * during shutdown, as shutting down the connectionFactory before shutting down the MatsFactory can lead to
 *
 * @author Kevin Mc Tiernan, 10-06-2020 - kmctiernan@gmail.com
 */
public class VerifyShutdownOrderUsingFactoryBeanTest extends AbstractFactoryBeanTestBase {

    @Test
    public void verifyShutdownOrder() {
        // :: Setup
        startSpring();

        // :: Act
        stopSpring();

        // :: Verify
        Map<String, Boolean> stoppedServices = _stoppedRegistry.getStoppedServices();

        Assert.assertEquals(stoppedServices.size(), 2);
        Assert.assertTrue(stoppedServices.containsKey(MatsFactoryVerifiableStopWrapper.class.getSimpleName()));
        Assert.assertTrue(stoppedServices.containsKey(MatsLocalVmActiveMqVerifiableStopWrapper.class.getSimpleName()));

        Assert.assertTrue(stoppedServices.get(MatsFactoryVerifiableStopWrapper.class.getSimpleName()));
        Assert.assertTrue(stoppedServices.get(MatsLocalVmActiveMqVerifiableStopWrapper.class.getSimpleName()));
    }
}
