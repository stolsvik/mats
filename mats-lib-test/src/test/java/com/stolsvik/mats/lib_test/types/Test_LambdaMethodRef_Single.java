package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests method references as the processing lambda, and whether we can invoke a method with a wider signature than the
 * specified classes.
 *
 * @author Endre Stølsvik - 2016-06-05 - http://endre.stolsvik.com
 */
public class Test_LambdaMethodRef_Single extends MatsBasicTest {
    private static final String MATCH_TYPES = ".MatchTypes";
    private static final String WIDE_TYPES = ".WideTypes";

    @Before
    public void setupService_MatchTypes_StaticMethodRef() {
        matsRule.getMatsFactory().single(SERVICE + MATCH_TYPES, DataTO.class, DataTO.class,
                Test_LambdaMethodRef_Single::lambda_MatchTypes);
    }

    @Before
    public void setupService_WideTypes_InstanceMethodRef() {
        matsRule.getMatsFactory().single(SERVICE + WIDE_TYPES, DataTO.class, DataTO.class, this::lambda_WideTypes);
    }

    private static DataTO lambda_MatchTypes(ProcessContext<DataTO> context, DataTO dto) {
        return new DataTO(dto.number * 2, dto.string + MATCH_TYPES);
    }

    private SubDataTO lambda_WideTypes(ProcessContext<?> context, Object dtoObject) {
        DataTO dto = (DataTO) dtoObject;
        return new SubDataTO(dto.number * 4, dto.string + WIDE_TYPES, dto.string + (Math.PI * 2));
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, SubDataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });

    }

    @Test
    public void matchTypes() {
        DataTO dto = new DataTO(Math.PI, "TheAnswer_1");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE + MATCH_TYPES)
                        .replyTo(TERMINATOR)
                        .request(dto, sto));

        // Wait synchronously for terminator to finish.
        Result<DataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + MATCH_TYPES), result.getData());
    }

    @Test
    public void wideTypes() {
        DataTO dto = new DataTO(Math.E, "TheAnswer_2");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE + WIDE_TYPES)
                        .replyTo(TERMINATOR)
                        .request(dto, sto));

        // Wait synchronously for terminator to finish.
        Result<SubDataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new SubDataTO(dto.number * 4, dto.string + WIDE_TYPES, dto.string + (Math.PI * 2)),
                result.getData());
    }

}
