package com.stolsvik.mats.spring.test.apptest_two_mf;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.spring.test.apptest_two_mf.Main_TwoMf.TestQualifier;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

@Component
public class TestApplicationBean {
    private static final Logger log = LoggerFactory.getLogger(TestApplicationBean.class);

    @Inject
    private MatsTestLatch _latch;

    @Inject
    @TestQualifier(endre="Elg")
    private MatsFactory _matsFactory;

    void run() {
        SpringTestDataTO dto = new SpringTestDataTO(Math.PI, "Data");
        SpringTestStateTO sto = new SpringTestStateTO(256, "State");
        _matsFactory.getDefaultInitiator().initiateUnchecked(
                msg -> msg.traceId("TraceId")
                        .from("FromId")
                        .to(Main_TwoMf.ENDPOINT_ID + ".single")
                        .replyTo(Main_TwoMf.ENDPOINT_ID + ".terminator", sto)
                        .request(dto));

        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        System.out.println("Reply: " + result.getData());
        System.out.println("State: " + result.getState());
    }
}
