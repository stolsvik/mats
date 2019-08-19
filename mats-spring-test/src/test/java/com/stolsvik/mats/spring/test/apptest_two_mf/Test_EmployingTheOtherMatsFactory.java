package com.stolsvik.mats.spring.test.apptest_two_mf;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.test.MatsTestProfile;
import com.stolsvik.mats.spring.test.apptest_two_mf.Mats_Endpoints.SingleEndpointUsingMatsClassMapping;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.util.RandomString;

/**
 * Sends a message via the {@link SingleEndpointUsingMatsClassMapping} endpoint, with is registered on the other
 * MatsFactory configured in {@link AppMain}.
 *
 * @author Endre Stølsvik 2019-06-06 21:53 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
@MatsTestProfile
@ContextConfiguration(classes = AppMain.class)
public class Test_EmployingTheOtherMatsFactory {
    private static final Logger log = LoggerFactory.getLogger(Test_EmployingTheOtherMatsFactory.class);

    @Inject
    @Qualifier("matsFactoryY") // This is the second MatsFactory
    private MatsFactory _matsFactory;

    @Inject
    private MatsTestLatch _latch;

    @Test
    public void test() {
        SpringTestDataTO dto = new SpringTestDataTO(4, "test");
        SpringTestStateTO sto = new SpringTestStateTO(1, "elg");
        _matsFactory.getDefaultInitiator().initiateUnchecked(msg -> {
            msg.traceId(RandomString.randomCorrelationId())
                    .from("TestInitiate")
                    .to(AppMain.ENDPOINT_ID + ".single")
                    .replyTo(AppMain.ENDPOINT_ID + ".terminator", sto)
                    .request(dto);
        });
        log.info("Sent message, going into wait on latch [" + _latch + "]");
        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new SpringTestDataTO(dto.number * 3, dto.string + ":single_on_class"),
                result.getData());
    }
}
