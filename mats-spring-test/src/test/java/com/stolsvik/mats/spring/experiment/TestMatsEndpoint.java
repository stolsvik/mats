package com.stolsvik.mats.spring.experiment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.SpringTestDataTO;
import com.stolsvik.mats.spring.test.SpringTestStateTO;
import com.stolsvik.mats.test.MatsTestLatch;

@Component
public class TestMatsEndpoint {
    private static final Logger log = LoggerFactory.getLogger(TestMatsEndpoint.class);

    public static final String ENDPOINT_ID = "mats.spring.TestMatsEndpoint";

    public String getHello() {
        return "World!";
    }

    MatsTestLatch _latch = new MatsTestLatch();

    /**
     * Test "Terminator" endpoint.
     */
    @MatsMapping(endpointId = ENDPOINT_ID + ".Terminator")
    public void springMatsTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
        _latch.resolve(msg, state);
    }

    /**
     * Test "Single" endpoint.
     */
    @MatsMapping(endpointId = ENDPOINT_ID + ".Single")
    public SpringTestDataTO springMatsSingleEndpoint(@Dto SpringTestDataTO msg) {
        return new SpringTestDataTO(msg.number * 2, msg.string + ":springMatsSingleEndpoint");
    }

    /**
     * Test "Single w/State" endpoint.
     */
    @MatsMapping(endpointId = ENDPOINT_ID + ".SingleWithState")
    public SpringTestDataTO springMatsSingleWithStateEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
        return new SpringTestDataTO(msg.number * state.number,
                msg.string + ":springMatsSingleWithStateEndpoint:" + state.string);
    }
}
