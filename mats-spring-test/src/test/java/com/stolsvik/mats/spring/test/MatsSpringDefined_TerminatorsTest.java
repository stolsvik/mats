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
import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.spring.Dto;
import com.stolsvik.mats.spring.MatsMapping;
import com.stolsvik.mats.spring.MatsTestContext.MatsTestInfrastructureContextInitializer;
import com.stolsvik.mats.spring.Sto;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = MatsTestInfrastructureContextInitializer.class)
@DirtiesContext
public class MatsSpringDefined_TerminatorsTest {
    public static final String ENDPOINT_ID = "mats.spring.MatsSpringDefined_TerminatorsTest";
    public static final String TERMINATOR = ".Terminator";
    public static final String TERMINATOR_DTO = ".Terminator_Dto";
    public static final String TERMINATOR_DTO_STO = ".Terminator_Dto_Sto";
    public static final String TERMINATOR_STO_DTO = ".Terminator_Sto_Dto";
    public static final String TERMINATOR_CONTEXT = ".Terminator_Context";
    public static final String TERMINATOR_CONTEXT_DTO = ".Terminator_Context_Dto";
    public static final String TERMINATOR_CONTEXT_DTO_STO = ".Terminator_Context_Dto_Sto";
    public static final String TERMINATOR_CONTEXT_STO_DTO = ".Terminator_Context_Sto_Dto";

    @Configuration
    static class TerminatorsConfiguration {
        @Inject
        private MatsTestLatch _latch;

        // == Permutations WITHOUT ProcessContext

        /**
         * Terminator endpoint (return value specified) - non-specified param, thus Dto.
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR)
        public void springMatsSingleEndpoint(SpringTestDataTO msg) {
            _latch.resolve(msg, null);
        }

        /**
         * Terminator endpoint (return value specified) - specified params: Dto.
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR_DTO)
        public void springMatsSingleEndpoint_Dto(@Dto SpringTestDataTO msg) {
            _latch.resolve(msg, null);
        }

        /**
         * "Single w/State"-endpoint (return value specified) - specified params: Dto, Sto
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR_DTO_STO)
        public void springMatsSingleWithStateEndpoint_Dto_Sto(@Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
            _latch.resolve(msg, state);
        }

        /**
         * "Single w/State"-endpoint (return value specified) - specified params: Sto, Dto
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR_STO_DTO)
        public void springMatsSingleWithStateEndpoint_Sto_Dto(@Sto SpringTestStateTO state, @Dto SpringTestDataTO msg) {
            _latch.resolve(msg, state);
        }

        // == Permutations WITH ProcessContext

        /**
         * Terminator endpoint (return value specified) - with Context - non-specified param, thus Dto.
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR_CONTEXT)
        public void springMatsSingleEndpoint_Context(ProcessContext<Void> context, SpringTestDataTO msg) {
            MatsTrace trace = context.getTrace();
            Assert.assertEquals("test_trace_id" + TERMINATOR_CONTEXT, trace.getTraceId());
            _latch.resolve(msg, null);
        }

        /**
         * Terminator endpoint (return value specified) - with Context - specified params: Dto
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR_CONTEXT_DTO)
        public void springMatsSingleEndpoint_Context_Dto(ProcessContext<Void> context, @Dto SpringTestDataTO msg) {
            MatsTrace trace = context.getTrace();
            Assert.assertEquals("test_trace_id" + TERMINATOR_CONTEXT_DTO, trace.getTraceId());
            _latch.resolve(msg, null);
        }

        /**
         * Terminator endpoint (return value specified) - with Context - specified params: Dto, Sto
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR_CONTEXT_DTO_STO)
        public void springMatsSingleEndpoint_Context_Dto_Sto(ProcessContext<Void> context,
                @Dto SpringTestDataTO msg, @Sto SpringTestStateTO state) {
            MatsTrace trace = context.getTrace();
            Assert.assertEquals("test_trace_id" + TERMINATOR_CONTEXT_DTO_STO, trace.getTraceId());
            _latch.resolve(msg, state);
        }

        /**
         * Terminator endpoint (return value specified) - with Context - specified params: Sto, Dto
         */
        @MatsMapping(endpointId = ENDPOINT_ID + TERMINATOR_CONTEXT_STO_DTO)
        public void springMatsSingleEndpoint_Context_Sto_Dto(ProcessContext<Void> context,
                @Sto SpringTestStateTO state, @Dto SpringTestDataTO msg) {
            MatsTrace trace = context.getTrace();
            Assert.assertEquals("test_trace_id" + TERMINATOR_CONTEXT_STO_DTO, trace.getTraceId());
            _latch.resolve(msg, state);
        }
    }

    @Inject
    private MatsInitiator _matsInitiator;

    @Inject
    private MatsTestLatch _latch;

    @Test
    public void sendToTerminator() {
        doTest(TERMINATOR, new SpringTestDataTO(Math.PI, "x" + TERMINATOR), null);
    }

    @Test
    public void sendToTerminator_Dto() {
        doTest(TERMINATOR_DTO, new SpringTestDataTO(Math.E, "x" + TERMINATOR_DTO), null);
    }

    @Test
    public void sendToTerminator_Dto_Sto() {
        doTest(TERMINATOR_DTO_STO, new SpringTestDataTO(Math.PI, "x" + TERMINATOR_DTO_STO),
                new SpringTestStateTO(4, "RequestState-A"));
    }

    @Test
    public void sendToTerminator_Sto_Dto() {
        doTest(TERMINATOR_STO_DTO, new SpringTestDataTO(Math.E, "x" + TERMINATOR_STO_DTO),
                new SpringTestStateTO(5, "RequestState-B"));
    }

    @Test
    public void sendToTerminator_Context() {
        doTest(TERMINATOR_CONTEXT, new SpringTestDataTO(Math.PI, "y" + TERMINATOR_CONTEXT), null);
    }

    @Test
    public void sendToTerminator_Context_Dto() {
        doTest(TERMINATOR_CONTEXT_DTO, new SpringTestDataTO(Math.E, "y" + TERMINATOR_CONTEXT_DTO), null);
    }

    @Test
    public void sendToTerminator_Context_Dto_Sto() {
        doTest(TERMINATOR_CONTEXT_DTO_STO, new SpringTestDataTO(Math.PI, "y" + TERMINATOR_CONTEXT_DTO_STO),
                new SpringTestStateTO(6, "RequestState-C"));
    }

    @Test
    public void sendToTerminator_Context_Sto_Dto() {
        doTest(TERMINATOR_CONTEXT_STO_DTO, new SpringTestDataTO(Math.E, "y" + TERMINATOR_CONTEXT_STO_DTO),
                new SpringTestStateTO(7, "RequestState-D"));
    }

    private void doTest(String epId, SpringTestDataTO dto, SpringTestStateTO requestSto) {
        _matsInitiator.initiate(
                init -> {
                    init.traceId("test_trace_id" + epId)
                            .from("FromId" + epId)
                            .to(ENDPOINT_ID + epId);
                    if (requestSto != null) {
                        init.send(dto, requestSto);
                    }
                    else {
                        init.send(dto);
                    }
                });

        Result<SpringTestDataTO, SpringTestStateTO> result = _latch.waitForResult();
        Assert.assertEquals(dto, result.getData());
        Assert.assertEquals(requestSto, result.getState());
    }
}