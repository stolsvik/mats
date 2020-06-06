package com.stolsvik.mats.spring.jms.tx;

import javax.sql.DataSource;

import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

import com.stolsvik.mats.spring.EnableMats;

/**
 * Testing Spring DB Transaction management, using DataSourceTransactionManager where the DataSource is wrapped before
 * put into Spring context.
 *
 * @author Endre Stølsvik 2020-06-05 00:10 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
public class Test_SpringManagedTx_H2Based_DataSourceTransactionaManager_Wrapped_All
        extends Test_SpringManagedTx_H2Based_DataSourceTransactionaManager {
    @Configuration
    @EnableMats
    static class SpringConfiguration_Test extends SpringConfiguration_DataSourceTxMgr {
        @Override
        protected DataSource optionallyWrapDataSource(DataSource dataSource) {
            return JmsMatsTransactionManager_JmsAndSpringManagedSqlTx.wrapLazyConnectionDatasource(dataSource);
        }
    }
}
