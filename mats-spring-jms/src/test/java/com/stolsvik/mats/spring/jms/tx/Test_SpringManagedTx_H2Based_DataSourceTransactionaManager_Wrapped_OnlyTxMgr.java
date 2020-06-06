package com.stolsvik.mats.spring.jms.tx;

import javax.sql.DataSource;

import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

import com.stolsvik.mats.spring.EnableMats;
import com.stolsvik.mats.spring.jms.tx.Test_SpringManagedTx_H2Based_DataSourceTransactionaManager.SpringConfiguration_DataSourceTxMgr;

/**
 * Testing Spring DB Transaction management, using DataSourceTransactionManager where the DataSource is wrapped only for
 * the TransactionManager - for the other users (JdbcTemplate etc), it is not wrapped.
 *
 * @author Endre Stølsvik 2020-06-05 00:10 - http://stolsvik.com/, endre@stolsvik.com
 */
@RunWith(SpringRunner.class)
public class Test_SpringManagedTx_H2Based_DataSourceTransactionaManager_Wrapped_OnlyTxMgr
        extends Test_SpringManagedTx_H2Based_AbstractResourceTransactionaManager {
    @Configuration
    @EnableMats
    static class SpringConfiguration_Test extends
            SpringConfiguration_DataSourceTxMgr {
        @Bean
        PlatformTransactionManager createDataSourceTransactionaManager(DataSource dataSource) {
            return new DataSourceTransactionManager(JmsMatsTransactionManager_JmsAndSpringManagedSqlTx
                    .wrapLazyConnectionDatasource(dataSource));
        }
    }
}
