package com.stolsvik.mats.lib_test.database;

import java.sql.Connection;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsDbTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.test.Rule_MatsWithDb.DatabaseException;

/**
 * Tests that if a Mats stage throws RTE, any SQL INSERT shall be rolled back.
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]
 *     [Service]  - inserts into database, then throws RTE (but also have test asserting that data inserts if we do NOT throw!)
 * [Terminator]  - checks that the msg is DLQed and data is not present (or test that it IS inserted if we do NOT throw!)
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_ExceptionInServiceRollbacksDb extends MatsDbTest {
    private static int WAIT_MILLIS = 500;

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    // Insert the data into the datatable
                    matsRule.insertDataIntoDataTable(context.getAttribute(Connection.class).get(),
                            "FromService:" + dto.string);
                    // ?: Are we requested to throw RTE?
                    if (dto.number == 1) {
                        // -> Yes, so do!
                        throw new RuntimeException("Should send message to DLQ after retries.");
                    }
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> matsTestLatch.resolve(sto, dto));
    }

    /**
     * Tests the infrastructure for checking that SERVICE's SQL inserts are rolled back upon RTE, by sending a message
     * using the same path (but with no RTE) which we assert that we DO receive, and where the data is inserted!
     */
    @Test
    public void checkTestInfrastructre() {
        String randomData = UUID.randomUUID().toString();

        // Create the SQL datatable - outside of the MATS transaction.
        matsRule.createDataTable();

        // Request that the SERVICE do NOT throw, providing the randomData to insert into the 'datatable'
        DataTO dto = new DataTO(0, randomData);
        StateTO sto = new StateTO(420, 420.024);

        // :: Send the request to SERVICE.
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> {
                    // :: Send the request
                    msg.traceId(randomId())
                            .from(INITIATOR)
                            .to(SERVICE)
                            .replyTo(TERMINATOR, sto)
                            .request(dto);
                });

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult(WAIT_MILLIS);
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());

        // Assert that the data inserted in SERVICE is actually in place.
        String dataFromDataTable = matsRule.getDataFromDataTable(matsRule.getNonTxConnection());
        Assert.assertEquals("FromService:" + randomData, dataFromDataTable);
    }

    /**
     * Tests that an SQL INSERT in the SERVICE will rolled back if SERVICE throws a RTE.
     */
    @Test
    public void exceptionInServiceShouldRollbackDb() {
        String randomData = UUID.randomUUID().toString();

        // Create the SQL datatable - outside of the MATS transaction.
        matsRule.createDataTable();

        // Request that the SERVICE throws, providing the randomData to insert into the 'datatable'
        DataTO dto = new DataTO(1, randomData);
        StateTO sto = new StateTO(420, 420.024);

        // :: Send the request to SERVICE.
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> {
                    // :: Send the request
                    msg.traceId(randomId())
                            .from(INITIATOR)
                            .to(SERVICE)
                            .replyTo(TERMINATOR, sto)
                            .request(dto);
                });

        // Wait for the DLQ
        MatsTrace dlqMatsTrace = matsRule.getDlqMessage(SERVICE);
        Assert.assertNotNull(dlqMatsTrace);
        Assert.assertEquals(SERVICE, dlqMatsTrace.getCurrentCall().getTo().getId());

        // Assert that the data inserted in SERVICE is NOT inserted!
        try {
            matsRule.getDataFromDataTable(matsRule.getNonTxConnection());
            Assert.fail("Should NOT have found any data in SQL Table 'datatable'!");
        }
        catch (DatabaseException e) {
            // Good that we came here! - the data should NOT have been received.
            log.info("We as expected dit NOT have the data in SQL Table 'datatable'!");
        }
    }
}
