package com.stolsvik.mats.spring.test.testapp_two_mf;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.spring.test.testapp_two_mf.TestMain_TwoMf.TestQualifier;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

@Component
public class TestApplicationBean {
    private static final Logger log = LoggerFactory.getLogger(TestApplicationBean.class);

    public static final String ENDPOINT_ID = "TestApp_TwoMf";

    @Inject
    @TestQualifier(endre="Elg")
    private MatsFactory _matsFactory;

    MatsTestLatch _latch = new MatsTestLatch();

    /**
     * Test "Terminator" endpoint.
     */
    @MatsMapping(endpointId = ENDPOINT_ID + ".terminator", matsFactoryCustomQualifierType = TestQualifier.class)
    public void springMatsTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
        _latch.resolve(state, msg);
    }

    /**
     * Test "Single" endpoint.
     */
    @MatsMapping(endpointId = ENDPOINT_ID + ".single")
    @Qualifier("matsFactoryX")
    public SpringTestDataTO springMatsSingleEndpoint(@Dto SpringTestDataTO msg) {
        return new SpringTestDataTO(msg.number * 2, msg.string + ":single");
    }

    void run() {
        SpringTestDataTO dto = new SpringTestDataTO(Math.PI, "Data");
        SpringTestStateTO sto = new SpringTestStateTO(256, "State");
        _matsFactory.createInitiator().initiateUnchecked(
                msg -> msg.traceId("TraceId")
                        .from("FromId")
                        .to(ENDPOINT_ID + ".single")
                        .replyTo(ENDPOINT_ID + ".terminator", sto)
                        .request(dto));

        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        System.out.println("Reply: " + result.getData());
        System.out.println("State: " + result.getState());
    }
}
