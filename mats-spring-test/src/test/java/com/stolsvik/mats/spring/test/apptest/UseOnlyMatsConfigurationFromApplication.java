package com.stolsvik.mats.spring.test.apptest;

import javax.inject.Inject;

import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.spring.test.TestSpringMatsFactoryProvider;
import com.stolsvik.mats.spring.test.testapp_two_mf.Mats_SingleEndpoint;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.spring.test.testapp_two_mf.Main_TwoMf;
import com.stolsvik.mats.spring.test.testapp_two_mf.Main_TwoMf.TestQualifier;
import com.stolsvik.mats.spring.test.testapp_two_mf.TestApplicationBean;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.util.RandomString;

/**
 * A test that points to only a specific @Configuration bean of an application, thus not taking up the entire
 * application - we have to provide the infrastructure (i.e. MatsFactories) in the test.
 * 
 * @author Endre Stølsvik 2019-06-06 21:53 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
public class UseOnlyMatsConfigurationFromApplication {
    private static final String TERMINATOR = "UseOnlyMatsConfigurationFromApplication.TERMINATOR";

    @Configuration
    // This is where we import the application's endpoint configurations
    @Import(Mats_SingleEndpoint.class)
    // Nobody else is doing it.
    @EnableMats
    public static class TestConfig {
        @Bean
        @Qualifier("matsFactoryX")
        protected MatsFactory matsFactory1() {
            return TestSpringMatsFactoryProvider.createJmsTxOnlyTestMatsFactory();
        }

        @Bean
        public MatsTestLatch latch() {
            return new MatsTestLatch();
        }

        @Inject
        private MatsTestLatch _latch;

        /**
         * Test "Terminator" endpoint where we send the result of testing the endpoint in the application.
         */
        @MatsMapping(endpointId = TERMINATOR, matsFactoryQualifierValue = "matsFactoryX")
        public void testTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
            _latch.resolve(state, msg);
        }
    }


    @Inject
    @Qualifier("matsFactoryX")
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
