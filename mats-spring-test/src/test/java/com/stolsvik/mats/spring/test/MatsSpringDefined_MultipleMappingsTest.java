package com.stolsvik.mats.spring.test;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.MatsSimpleTestContext.MatsTestInfrastructureConfiguration;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.spring.test.MatsSpringDefined_MultipleMappingsTest.MultipleMappingsConfiguration;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests that {@link MatsMapping @MatsMapping} works when being present multiple times.
 *
 * @author Endre Stølsvik - 2016-08-07 - http://endre.stolsvik.com
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = { MatsTestInfrastructureConfiguration.class, MultipleMappingsConfiguration.class })
@DirtiesContext
public class MatsSpringDefined_MultipleMappingsTest {
    public static final String ENDPOINT_ID = "mats.spring.MatsSpringDefined_MultipleMappingsTest";
    public static final String TERMINATOR1 = ".TERMINATOR1";
    public static final String TERMINATOR2 = ".TERMINATOR2";
    public static final String SINGLE1 = ".Single1";
    public static final String SINGLE2 = ".Single2";

    @Configuration
    static class MultipleMappingsConfiguration {
        @Inject
        private MatsTestLatch _latch;

        @MatsMapping(endpointId = ENDPOINT_ID + SINGLE1)
        @MatsMapping(endpointId = ENDPOINT_ID + SINGLE2)
        public SpringTestDataTO springMatsSingleEndpoint(ProcessContext<SpringTestDataTO> ctx, SpringTestDataTO msg) {
            return new SpringTestDataTO(msg.number * 2, msg.string + ctx.getEndpointId());
        }

        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR1)
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR2)
        public void springMatsSingleEndpoint_Dto(ProcessContext<Void> ctx,
                @Dto SpringTestDataTO msg, @Sto SpringTestStateTO sto) {
            _latch.resolve(new SpringTestDataTO(msg.number * 3, msg.string + ctx.getEndpointId()), sto);
        }
    }

    @Inject
    private MatsInitiator _matsInitiator;

    @Inject
    private MatsTestLatch _latch;

    @Test
    public void single1_terminator1() {
        SpringTestDataTO dto = new SpringTestDataTO(2, "five");
        SpringTestStateTO sto = new SpringTestStateTO(5, "two");
        _matsInitiator.initiate(init -> {
            init.traceId("test_trace_id:" + Math.random())
                    .from(MatsSpringDefined_MultipleMappingsTest.class.getSimpleName())
                    .to(ENDPOINT_ID + SINGLE1)
                    .replyTo(ENDPOINT_ID + TERMINATOR1, sto)
                    .request(dto);
        });

        Result<SpringTestDataTO, SpringTestStateTO> result = _latch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2 * 3, dto.string + ENDPOINT_ID + SINGLE1
                + ENDPOINT_ID + TERMINATOR1), result.getData());
    }

    @Test
    public void single2_terminator2() {
        SpringTestDataTO dto = new SpringTestDataTO(6, "six");
        SpringTestStateTO sto = new SpringTestStateTO(7, "seven");
        _matsInitiator.initiate(init -> {
            init.traceId("test_trace_id:" + Math.random())
                    .from(MatsSpringDefined_MultipleMappingsTest.class.getSimpleName())
                    .to(ENDPOINT_ID + SINGLE2)
                    .replyTo(ENDPOINT_ID + TERMINATOR2, sto)
                    .request(dto);
        });

        Result<SpringTestDataTO, SpringTestStateTO> result = _latch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new SpringTestDataTO(dto.number * 2 * 3, dto.string + ENDPOINT_ID + SINGLE2
                + ENDPOINT_ID + TERMINATOR2), result.getData());
    }

}
