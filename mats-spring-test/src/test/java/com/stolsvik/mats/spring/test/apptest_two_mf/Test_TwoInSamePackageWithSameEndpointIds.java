package com.stolsvik.mats.spring.test.apptest_two_mf;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.ComponentScanExcludingConfigurationForTest;
import com.stolsvik.mats.spring.ConfigurationForTest;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.MatsTestProfile;
import com.stolsvik.mats.spring.test.apptest_two_mf.AppMain.TestQualifier;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.util.RandomString;

/**
 * This is a copy of {@link Test_UseFullApplicationConfiguration}, which employs the same TERMINATOR endpoint Id, just
 * to point out that the issue where @ComponentScan picks up the inner static @Configuration classes of tests, is
 * <b>not</b> present when employing the two special variants
 * {@link ComponentScanExcludingConfigurationForTest @ComponentScanExcludingConfigurationForTest} and
 * {@link ConfigurationForTest @ConfigurationForTest} instead of the standard ones. (Mats will refuse to add two
 * endpoints having the same endpointId to a single MatsFactory).
 *
 * @author Endre Stølsvik 2019-06-06 21:53 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
@MatsTestProfile
public class Test_TwoInSamePackageWithSameEndpointIds {
    private static final Logger log = LoggerFactory.getLogger(Test_TwoInSamePackageWithSameEndpointIds.class);

    @ConfigurationForTest
    @Import(AppMain.class)
    public static class TestConfig {
        @Inject
        private MatsTestLatch _latch;

        @MatsMapping(endpointId = Test_UseFullApplicationConfiguration.TERMINATOR, matsFactoryCustomQualifierType = TestQualifier.class)
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
                    .replyTo(Test_UseFullApplicationConfiguration.TERMINATOR, null)
                    .request(dto);
        });
        log.info("Sent message, going into wait on latch [" + _latch + "]");
        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2, dto.string + ":single"), result.getData());
    }
}
