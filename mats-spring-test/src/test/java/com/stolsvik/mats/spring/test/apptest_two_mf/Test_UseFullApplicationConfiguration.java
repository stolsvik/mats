package com.stolsvik.mats.spring.test.apptest_two_mf;

import javax.inject.Inject;
import javax.jms.ConnectionFactory;

import com.stolsvik.mats.spring.ConfigurationForTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.jms.factories.JmsSpringConnectionFactoryProducer;
import com.stolsvik.mats.spring.jms.factories.MatsProfiles;
import com.stolsvik.mats.spring.test.MatsTestProfile;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.spring.test.apptest_two_mf.AppMain.TestQualifier;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.util.RandomString;

/**
 * A test that points to a full application configuration, effectively taking up the entire application, but overriding
 * the JMS {@link ConnectionFactory}s that the application's {@link MatsFactory}s are using with test-variants employing
 * a local VM Active MQ instance: The trick is the use of {@link JmsSpringConnectionFactoryProducer} inside the
 * application to specify which JMS {@link ConnectionFactory} that should be used, which the Mats' test infrastructure
 * can override by specifying the Spring Profile {@link MatsProfiles#PROFILE_MATS_TEST} - which is done using the
 * {@link MatsTestProfile @MatsTestProfile} annotation.
 * 
 * @author Endre Stølsvik 2019-06-06 21:53 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
// This overrides the configured ConnectionFactories in the app to be LocalVM testing instances.
@MatsTestProfile
public class Test_UseFullApplicationConfiguration {
    private static final Logger log = LoggerFactory.getLogger(Test_UseFullApplicationConfiguration.class);
    static final String TERMINATOR = "Test.TERMINATOR";

    @ConfigurationForTest
    // This is where we import the application's main configuration class
    // 1. It is annotated with @EnableMats
    // 2. It configures two ConnectionFactories, and two MatsFactories.
    // 3. It configures classpath scanning, and thus gets the Mats endpoints configured.
    @Import(AppMain.class)
    public static class TestConfig {
        @Inject
        private MatsTestLatch _latch;

        /**
         * Test "Terminator" endpoint where we send the result of testing the endpoint in the application.
         */
        @MatsMapping(endpointId = TERMINATOR, matsFactoryCustomQualifierType = TestQualifier.class)
        public void testTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
            log.info("Got result, resolving latch [" + _latch + "]!");
            _latch.resolve(state, msg);
        }
    }

    @Inject
    @TestQualifier(name = "Endre Stølsvik")
    private MatsFactory _matsFactory;

    @Inject
    private MatsTestLatch _latch;

    @Test
    public void test() {
        SpringTestDataTO dto = new SpringTestDataTO(4, "test");
        _matsFactory.getDefaultInitiator().initiateUnchecked(msg -> {
            msg.traceId(RandomString.randomCorrelationId())
                    .from("TestInitiate")
                    .to(AppMain.ENDPOINT_ID + ".single")
                    .replyTo(TERMINATOR, null)
                    .request(dto);
        });
        log.info("Sent message, going into wait on latch [" + _latch + "]");
        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2, dto.string + ":single"), result.getData());
    }
}
