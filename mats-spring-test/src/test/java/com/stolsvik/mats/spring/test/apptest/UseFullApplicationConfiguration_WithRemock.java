package com.stolsvik.mats.spring.test.apptest;

import javax.inject.Inject;
import javax.jms.ConnectionFactory;

import com.stolsvik.mats.spring.MatsSpringAnnotationRegistration;
import com.stolsvik.mats.spring.test.apptest.UseFullApplicationConfiguration_WithRemock.TestConfig;
import com.stolsvik.mats.spring.test.testapp_two_mf.Mats_SingleEndpoint;
import no.saua.remock.DisableLazyInit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.BootstrapWith;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.MatsTestProfile;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.spring.test.testapp_two_mf.Main_TwoMf;
import com.stolsvik.mats.spring.test.testapp_two_mf.Main_TwoMf.TestQualifier;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.util.RandomString;

import no.saua.remock.RemockBootstrapper;

/**
 * As identical to {@link UseFullApplicationConfiguration} as possible, but now using
 * <a href="https://github.com/ksaua/remock">Remock</a>. The issue when using Remock is that this library puts all beans
 * into <i>lazy initialization</i> mode to make tests (in particular those pointing to the entire application's
 * dependency injection configuration) as fast as possible by only firing up beans that are actually "touched" by the
 * tests. There was an issue with some of Mats' SpringConfig elements relying on eager init - and this test tries to
 * weed out these dependencies.
 *
 * @author Endre Stølsvik 2019-06-25 23:31 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
// Using Remock
@BootstrapWith(RemockBootstrapper.class)
// This overrides the configured ConnectionFactories in the app to be LocalVM testing instances.
// @MatsTestProfile              // <- Virker ikke
// @ActiveProfiles("mats-test")  // <- Virker ikke
@DisableLazyInit({TestConfig.class, ConnectionFactory.class, Mats_SingleEndpoint.class})
public class UseFullApplicationConfiguration_WithRemock {
    private static final String TERMINATOR = "UseFullApplicationConfiguration.TERMINATOR";

    // Must set the System Property variant of MatsScenario, since evidently remock wipes the @ActiveProfiles.
    {
        System.setProperty("mats.test", "");
    }

    @AfterClass
    public static void clearMatsTest() {
        System.clearProperty("mats.test");
    }

    // This is not enough to get proper dependency, as it is registered too late, and thus won't be started by
    // the MatsSpringAnnotationRegistration when that has its SmartLifeCycle.start() invoked.
    @Inject
    private Mats_SingleEndpoint _dependency;

    @Configuration
    // This is where we import the application's main configuration class
    // 1. It is annotated with @EnableMats
    // 2. It configures two ConnectionFactories, and two MatsFactories.
    // 3. It configures classpath scanning, and thus gets the Mats endpoints configured.
    @Import(Main_TwoMf.class)
    public static class TestConfig {
        @Inject
        private MatsTestLatch _latch;

        /**
         * Test "Terminator" endpoint where we send the result of testing the endpoint in the application.
         */
        @MatsMapping(endpointId = TERMINATOR, matsFactoryCustomQualifierType = TestQualifier.class)
        public void testTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
            _latch.resolve(state, msg);
        }
    }

    @Inject
    @TestQualifier(endre = "Elg")
    private MatsFactory _matsFactory;

    @Inject
    private MatsTestLatch _latch;

    @Test
    public void test() {
        SpringTestDataTO dto = new SpringTestDataTO(4, "test");
        _matsFactory.getDefaultInitiator().initiateUnchecked(msg -> {
            msg.traceId(RandomString.randomCorrelationId())
                    .from("TestInitiate")
                    .to(Main_TwoMf.ENDPOINT_ID + ".single")
                    .replyTo(TERMINATOR, null)
                    .request(dto);
        });
        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2, dto.string + ":single"), result.getData());
    }

}
