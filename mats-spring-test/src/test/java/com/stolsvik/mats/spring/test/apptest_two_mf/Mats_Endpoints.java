package com.stolsvik.mats.spring.test.apptest_two_mf;

import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsClassMapping;
import com.stolsvik.mats.spring.MatsClassMapping.Stage;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.apptest_two_mf.AppMain.TestQualifier;
import com.stolsvik.mats.spring.test.mapping.SpringTestDataTO;
import com.stolsvik.mats.spring.test.mapping.SpringTestStateTO;
import com.stolsvik.mats.test.MatsTestLatch;

/**
 * @author Endre Stølsvik 2019-06-17 23:48 - http://stolsvik.com/, endre@stolsvik.com
 */
@Component
public class Mats_Endpoints {
    public static final String THROW = "ThrowIt!";

    @Inject
    private MatsTestLatch _latch;

    /**
     * Test "Single" endpoint.
     */
    @MatsMapping(AppMain.ENDPOINT_ID + ".single")
    @Qualifier("matsFactoryX")
    public SpringTestDataTO springMatsSingleEndpoint(@Dto SpringTestDataTO msg) throws MatsRefuseMessageException {
        if (msg.string.equals(THROW)) {
            throw new MatsRefuseMessageException("Got asked to throw, so that we do!");
        }
        return new SpringTestDataTO(msg.number * 2, msg.string + ":single");
    }

    /**
     * Test "Terminator" endpoints.
     */
    @MatsMapping(endpointId = AppMain.ENDPOINT_ID + ".terminator", matsFactoryCustomQualifierType = TestQualifier.class)
    @MatsMapping(endpointId = AppMain.ENDPOINT_ID + ".terminator", matsFactoryQualifierValue = "matsFactoryY")
    public void springMatsTerminatorEndpoint(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
        _latch.resolve(state, msg);
    }

    /**
     * Test Multi-Stage @MatsClassMapping endpoint.
     */
    @MatsClassMapping(AppMain.ENDPOINT_ID + ".multi")
    @TestQualifier(name = "Endre Stølsvik") // This is the same as "matsFactoryX", the first.
    static class MultiEndPoint {
        @Inject
        private AtomicInteger _atomicInteger;

        private ProcessContext<SpringTestDataTO> _context;

        private double _statePi;
        private SpringTestDataTO _stateObject;

        @Stage(Stage.INITIAL)
        void initialStage(SpringTestDataTO in) {
            _statePi = Math.PI;
            _stateObject = new SpringTestDataTO(Math.E, "This is state.");

            // Perform some advanced stuff with out Dependency Injected Service..!
            _atomicInteger.incrementAndGet();

            _context.request(AppMain.ENDPOINT_ID + ".single", new SpringTestDataTO(in.number, in.string));
        }

        @Stage(10)
        SpringTestDataTO finalStage(SpringTestDataTO in) {
            Assert.assertEquals(Math.PI, _statePi, 0d);
            Assert.assertEquals(new SpringTestDataTO(Math.E, "This is state."), _stateObject);

            return new SpringTestDataTO(in.number * 3, in.string + ":multi");
        }
    }

    /**
     * Test Single-Stage @MatsClassMapping endpoint.
     * <p/>
     * Notice how we're using the same endpointId as {@link Mats_Endpoints#springMatsSingleEndpoint(SpringTestDataTO)},
     * but this is OK since this is on a different MatsFactory.
     */
    @MatsClassMapping(endpointId = AppMain.ENDPOINT_ID + ".single", matsFactoryQualifierValue = "matsFactoryY")
    static class SingleEndpointUsingMatsClassMapping {
        @Inject
        private AtomicInteger _atomicInteger;

        @Stage(0)
        SpringTestDataTO initialStage(SpringTestDataTO in) {
            _atomicInteger.incrementAndGet();
            return new SpringTestDataTO(in.number * 3, in.string + ":single_on_class");
        }
    }

}
