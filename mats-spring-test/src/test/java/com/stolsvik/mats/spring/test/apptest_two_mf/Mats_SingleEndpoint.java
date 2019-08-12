package com.stolsvik.mats.spring.test.apptest_two_mf;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;

/**
 * @author Endre Stølsvik 2019-06-17 23:48 - http://stolsvik.com/, endre@stolsvik.com
 */
@Component
public class Mats_SingleEndpoint {
    public static final String THROW = "ThrowIt!";

    /**
     * Test "Single" endpoint.
     */
    @MatsMapping(endpointId = Main.ENDPOINT_ID + ".single")
    @Qualifier("matsFactoryX")
    public SpringTestDataTO springMatsSingleEndpoint(@Dto SpringTestDataTO msg) throws MatsRefuseMessageException {
        if (msg.string.equals(THROW)) {
            throw new MatsRefuseMessageException("Got asked to throw, so that we do!");
        }
        return new SpringTestDataTO(msg.number * 2, msg.string + ":single");
    }

}
