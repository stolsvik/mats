package com.stolsvik.mats.spring.test.mapping;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsClassMapping;
import com.stolsvik.mats.spring.MatsClassMapping.Stage;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.MatsSimpleTestContext;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * @author Endre Stølsvik 2019-08-13 22:13 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
@MatsSimpleTestContext
public class MatsSpringDefined_MatsStagedOnClassTest {
    private static final Logger log = LoggerFactory.getLogger(MatsSpringDefined_MatsStagedOnClassTest.class);

    private static final String SERVICE_MAIN = "test.AppMain";
    private static final String SERVICE_LEAF = "test.Leaf";
    private static final String TERMINATOR = "test.Terminator";

    @Configuration
    public static class Config {
        @Inject
        private MatsTestLatch _latch;

        @MatsMapping(SERVICE_LEAF)
        public SpringTestDataTO springMatsSingleEndpoint(SpringTestDataTO msg) {
            return new SpringTestDataTO(msg.number * 2, msg.string + ':' + SERVICE_LEAF);
        }

        @MatsMapping(TERMINATOR)
        public void springMatsTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
            _latch.resolve(state, msg);
        }

        @Bean
        protected SpringTestDataTO someObject() {
            return new SpringTestDataTO(2, "to");
        }

        @Bean
        protected SpringTestStateTO someOtherObject() {
            return new SpringTestStateTO(5, "fem");
        }
    }

    /*
     * Add check that there are no MatsMappings or MatsEndpointConfigs inside a MatsClassMapping
     */

    @MatsClassMapping(SERVICE_MAIN)
    @Configuration // This must be here so that this Spring component is automatically picked up by test runner.
    public static class MatsStagedClass {

        // === DEPENDENCIES INJECTED BY SPRING

        @Inject
        private SpringTestDataTO _someObject;

        @Inject
        private SpringTestStateTO _someOtherObject;

        // === MATS' ProcessContext FOR CURRENT MESSAGE INJECTED BY MATS' "SpringConfig" LIBRARY

        private ProcessContext<SpringTestDataTO> _context;

        // === STATE VARIABLES, kept by Mats between the different stages of the endpoint.

        private int _someStateInt;
        private String _someStateString;
        private SpringTestDataTO _someStateObject;

        // === ENDPOINT STAGES

        @Stage(Stage.INITIAL)
        void receiveAndCheckValidity(SpringTestDataTO in) {
            // Assert that state is empty (null, zero)
            Assert.assertEquals(0, _someStateInt);
            Assert.assertNull(_someStateString);
            Assert.assertNull(_someStateObject);

            // Set some state for next stage.
            _someStateInt = -10;
            _someStateString = "SetFromInitial";
            _someStateObject = null;

            // Do a request to a service
            _context.request(SERVICE_LEAF, new SpringTestDataTO(in.number, in.string));
        }

        @Stage(10)
        void evaluateSomeThings(SpringTestDataTO in) {
            // Assert that state is kept from previous stage
            Assert.assertEquals(-10, _someStateInt);
            Assert.assertEquals("SetFromInitial", _someStateString);
            Assert.assertNull(_someStateObject);

            // Set some state for next stage.
            _someStateInt = 10;
            _someStateString = "SetFromStageA";
            _someStateObject = new SpringTestDataTO(57473, "state");

            // Jump to next stage: Notice the use of a different DTO here (just using the STO since it was here)
            _context.next(new SpringTestStateTO((int) in.number * 3, in.string + ":next"));
        }

        @Stage(20)
        void processSomeStuff(@Dto SpringTestStateTO in, String anotherParameterMaybeUsedForTesting) {
            // Assert that state is kept from previous stage
            Assert.assertEquals(10, _someStateInt);
            Assert.assertEquals("SetFromStageA", _someStateString);
            Assert.assertEquals(new SpringTestDataTO(57473, "state"), _someStateObject);

            // Set some state for next stage.
            _someStateInt = 20;
            _someStateString = "SetFromStageB";
            _someStateObject = new SpringTestDataTO(314159, "pi * 100.000");

            // Do a stash, to check the ProcessContext wrapping (not handled, should not need).
            _context.stash();

            // Do a request to a service
            _context.request(SERVICE_LEAF, new SpringTestDataTO(in.numero, in.cuerda));
        }

        @Stage(30)
        SpringTestDataTO processMoreThenReply(boolean primitiveBooleanParameter,
                byte byteP, short shortP, int intP, long longP, @Dto SpringTestDataTO in,
                float floatP, double doubleP) {
            // Assert that state is kept from previous stage
            Assert.assertEquals(20, _someStateInt);
            Assert.assertEquals("SetFromStageB", _someStateString);
            Assert.assertEquals(new SpringTestDataTO(314159, "pi * 100.000"), _someStateObject);

            // Reply to caller with our amazing result.
            return new SpringTestDataTO(in.number * 5, in.string + ':' + SERVICE_MAIN);
        }
    }

    @Inject
    private MatsInitiator _matsInitiator;

    @Inject
    private MatsTestLatch _latch;

    @Test
    public void doTest() {
        SpringTestStateTO sto = new SpringTestStateTO(256, "State");
        SpringTestDataTO dto = new SpringTestDataTO(12, "tolv");
        _matsInitiator.initiateUnchecked(
                init -> {
                    init.traceId("test_trace_id")
                            .from("FromId")
                            .to(SERVICE_MAIN)
                            .replyTo(TERMINATOR, sto);
                    init.request(dto);
                });

        Result<SpringTestStateTO, SpringTestDataTO> result = _latch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2 * 3 * 2 * 5,
                dto.string + ':' + SERVICE_LEAF + ":next" + ':' + SERVICE_LEAF + ':' + SERVICE_MAIN),
                result.getData());
    }

}
