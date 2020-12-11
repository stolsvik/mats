package com.stolsvik.mats.spring.test.apptest1;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.test.SpringTestDataTO;

/**
 *
 * @author Endre Stølsvik 2020-11-16 21:10 - http://stolsvik.com/, endre@stolsvik.com
 */
@Service
public class AppEndpoint_LeafServices {

    @Inject
    private AppServiceCalculator _calculator;

    /**
     * Test "leaf service 1".
     */
    @MatsMapping(AppMain_MockAndTestingHarnesses.ENDPOINT_ID_LEAFSERVICE1)
    public SpringTestDataTO leafService1(@Dto SpringTestDataTO msg) {
        return new SpringTestDataTO(_calculator.multiplyByΠ(msg.number),
                msg.string + ':' + AppMain_MockAndTestingHarnesses.ENDPOINT_ID_LEAFSERVICE1);
    }

    /**
     * Test "leaf service 2".
     */
    @MatsMapping(AppMain_MockAndTestingHarnesses.ENDPOINT_ID_LEAFSERVICE2)
    public SpringTestDataTO leafService2(@Dto SpringTestDataTO msg) {
        return new SpringTestDataTO(_calculator.multiplyByΦ(msg.number),
                msg.string + ':' + AppMain_MockAndTestingHarnesses.ENDPOINT_ID_LEAFSERVICE2);
    }
}
