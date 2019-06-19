package com.stolsvik.mats.spring.test.testapp_two_mf;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.spring.test.testapp_two_mf.Main_TwoMf.TestQualifier;
import com.stolsvik.mats.test.MatsTestLatch;

/**
 * @author Endre Stølsvik 2019-06-17 23:50 - http://stolsvik.com/, endre@stolsvik.com
 */
@Component
public class Mats_TerminatorEndpoints {
    @Inject
    private MatsTestLatch _latch;

    /**
     * Test "Terminator" endpoints.
     */
    @MatsMapping(endpointId = Main_TwoMf.ENDPOINT_ID
            + ".terminator", matsFactoryCustomQualifierType = TestQualifier.class)
    @MatsMapping(endpointId = Main_TwoMf.ENDPOINT_ID + ".terminator", matsFactoryQualifierValue = "matsFactoryY")
    public void springMatsTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
        _latch.resolve(state, msg);
    }
}
