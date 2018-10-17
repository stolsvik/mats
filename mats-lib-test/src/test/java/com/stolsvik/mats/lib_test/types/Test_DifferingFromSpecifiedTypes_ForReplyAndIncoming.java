package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * It is possible for a service to reply with a more specific (i.e. sub class, or "narrower") than what it specifies.
 * (This can be of use if one consumer of a service needs some extra info compared to the standard, e.g. based on the
 * Request DTO having some flag set, the service can add in the extra info. One can of course just leave some fields
 * null on the sole Reply DTO in use, but it might be of more information to stick the extra fields in a sub class Reply
 * DTO, which then can be JavaDoc'ed separately).
 * <p>
 * This class tests that option, by having a service that can reply with both the {@link DataTO} and the sub class
 * {@link SubDataTO}, and two different terminators: One expecting a {@link DataTO} and one expecting a
 * {@link SubDataTO}. Three combinatios are tested: Reply with SubDataTO, then Receiving SubDataTO, and Receiving
 * DataTO. And then Reply with DataTO, Receiving SubDataTO. Replying with DataTO, Receiving DataTO is the normal case,
 * and is tested many times other places.
 *
 * @author Endre Stølsvik - 2016-06-04 - http://endre.stolsvik.com
 */
public class Test_DifferingFromSpecifiedTypes_ForReplyAndIncoming extends MatsBasicTest {
    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    if (dto.string.equals("SubDataTO")) {
                        // Replying with Covariant (i.e. more specialized, narrower) type than specified
                        return new SubDataTO(dto.number * 2, dto.string + ":FromService_SubDataTO",
                                "SubDataTO_Specific");
                    }
                    else {
                        // Reply with the ordinary DataDto
                        return new DataTO(dto.number * 2, dto.string + ":FromService_DataTO");
                    }
                });
    }

    @Before
    public void setupTerminator_DataDto() {
        // Expecting/Deserializing into the base/parent ("wide") type:
        matsRule.getMatsFactory().terminator(TERMINATOR + ".DataTO", StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Before
    public void setupTerminator_SubDataDto() {
        // Expecting/Deserializing into the child ("narrow"/"specific") type:
        matsRule.getMatsFactory().terminator(TERMINATOR + ".SubDataTO", StateTO.class, SubDataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    MatsInitiator _initiator;

    @Before
    public void getInitiator() {
        _initiator = matsRule.getMatsFactory().createInitiator();
    }

    @Test
    public void serviceRepliesWithMoreSpecificDtoThanSpecified_AndTerminatorExpectsThis() {
        // Specifies that Service shall reply with SubDataDto (more specific reply DTO than specified)
        DataTO dto = new DataTO(Math.E, "SubDataTO");
        StateTO sto = new StateTO(420, 420.024);
        _initiator.initiateUnchecked((msg) -> msg.traceId(randomId())
                .from(INITIATOR)
                .to(SERVICE)
                .replyTo(TERMINATOR + ".SubDataTO", sto)
                .request(dto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, SubDataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new SubDataTO(dto.number * 2, dto.string + ":FromService_SubDataTO", "SubDataTO_Specific"),
                result.getData());
    }

    @Test
    public void serviceRepliesWithMoreSpecificDtoThanSpecified_ButTerminatorExpectsBaseDto_ThusTooManyFields() {
        // Specifies that Service shall reply with SubDataDto (more specific reply DTO than specified)
        DataTO dto = new DataTO(Math.PI, "SubDataTO");
        StateTO sto = new StateTO(420, 420.024);
        _initiator.initiateUnchecked((msg) -> msg.traceId(randomId())
                .from(INITIATOR)
                .to(SERVICE)
                .replyTo(TERMINATOR + ".DataTO", sto)
                .request(dto));

        // Wait synchronously for terminator to finish.
        Result<DataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService_SubDataTO"), result.getData());
    }

    @Test
    public void serviceRepliesWithBaseDtoAsSpecified_ButTerminatorExpectsSubDataDto_ThusMissingFields() {
        // Specifies that Service shall reply with DataDto (the DTO it specifies it will reply with)
        DataTO dto = new DataTO(Math.E * Math.PI, "DataTO");
        StateTO sto = new StateTO(420, 420.024);
        _initiator.initiateUnchecked((msg) -> msg.traceId(randomId())
                .from(INITIATOR)
                .to(SERVICE)
                .replyTo(TERMINATOR + ".SubDataTO", sto)
                .request(dto));

        // Wait synchronously for terminator to finish.
        Result<SubDataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new SubDataTO(dto.number * 2, dto.string + ":FromService_DataTO", null),
                result.getData());
    }
}
