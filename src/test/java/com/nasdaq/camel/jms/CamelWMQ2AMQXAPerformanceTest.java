package com.nasdaq.camel.jms;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jms.AtomikosConnectionFactoryBean;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.mq.jms.MQXAConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import java.util.EventObject;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.UriEndpointComponent;
import org.apache.camel.management.event.ExchangeSentEvent;
import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.camel.support.EventNotifierSupport;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Scope;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.jta.JtaTransactionManager;

/**
 * Test code based on tests published in the conversation at:
 * http://camel.465427.n5.nabble.com/Camel-JMS-Performance-is-ridiculously-worse-than-pure-Spring-DMLC-td5716998.html
 *
 * @author Ola Theander <ola.theander@nasdaqomx.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class CamelWMQ2AMQXAPerformanceTest extends CamelSpringTestSupport {

    private static final String PAYLOAD
            = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxx";

    @Configuration
    static class Config {

        @Bean(name = "brokerService", destroyMethod = "stop")
        public BrokerService createBrokerService() throws Exception {
            final BrokerService broker = new BrokerService();
            broker.setPersistent(true);
            broker.setUseJmx(false);
            broker.addConnector("tcp://localhost:61616");
            broker.start();
            return broker;
        }

        @Bean(name = "activemq")
        public JmsComponent getActiveMQBean() {
            return ActiveMQComponent.activeMQComponent("tcp://localhost:61616");
        }

        @Bean(name = "amqxa")
        @DependsOn("brokerService")
        public UriEndpointComponent createAmqXaEndpoint(BeanFactory beanFactory, JtaTransactionManager jtam) {
            final ActiveMQXAConnectionFactory amqConnectionFactory = new ActiveMQXAConnectionFactory();
            /**
             * jms.prefetchPolicy.all=0 to avoid the rollback:
             *
             * JmsConsumer[INTERNAL_MT_IN.QUEUE]] DEBUG
             * o.a.a.ActiveMQMessageConsumer - on close, rollback duplicate:
             * ID:SE10WS01439-50920-1413812301395-1:18:2:1:1
             */
            // JmsConsumer[INTERNAL_MT_IN.QUEUE]] DEBUG o.a.a.ActiveMQMessageConsumer - on close,
            // rollback duplicate: ID:SE10WS01439-50920-1413812301395-1:18:2:1:1
            final String brokerURL = String.format("tcp://%s:%d?jms.prefetchPolicy.all=0",
                    "localhost",
                    61616);
            amqConnectionFactory.setBrokerURL(brokerURL);
            final AtomikosConnectionFactoryBean atomikosCF = createConnectionFactory(
                    beanFactory,
                    amqConnectionFactory,
                    8,
                    "atomikos_amqxa");

            final JmsComponent jms = new JmsComponent();
            jms.setConnectionFactory(atomikosCF);
            jms.setTransactionManager(jtam);
            jms.setTransacted(true);
            jms.setReceiveTimeout(2000);
            jms.setCacheLevelName("CACHE_NONE");
            return jms;
        }

        @Bean(name = "wmqxa")
        public JmsComponent createWmqXaEndpoint(MQXAConnectionFactory wmqConnectionFactory,
                BeanFactory beanFactory,
                JtaTransactionManager jtam) {
            final AtomikosConnectionFactoryBean atomikosCF = createConnectionFactory(
                    beanFactory,
                    wmqConnectionFactory,
                    8,
                    "MQSeries_XA_RMI_wmqxa");

            final JmsComponent jms = new JmsComponent();
            jms.setConnectionFactory(atomikosCF);
            jms.setTransacted(true);
            jms.setTransactionManager(jtam);
            jms.setReceiveTimeout(2000);
            return jms;
        }

        @Bean(name = "wmq")
        @Qualifier(value = "wmqConnectionFactory")
        public JmsComponent createWmqEndpoint(MQConnectionFactory wmqConnectionFactory) {

            final JmsComponent jms = new JmsComponent();
            jms.setConnectionFactory(wmqConnectionFactory);
            jms.setTransacted(false);
            jms.setReceiveTimeout(2000);
            return jms;
        }

        @Bean(name = "wmqConnectionFactory")
        public MQConnectionFactory getConnectionFactory() throws JMSException {
            final MQConnectionFactory wmqConnectionFactory = new MQConnectionFactory();
            wmqConnectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            wmqConnectionFactory.setHostName(
                    "10.20.28.12");
            wmqConnectionFactory.setPort(
                    1414);
            wmqConnectionFactory.setQueueManager(
                    "VPA12");
            wmqConnectionFactory.setChannel(
                    "SMRV.TST");
            return wmqConnectionFactory;
        }

        @Bean
        public MQXAConnectionFactory createWmqXaConnectionFactory() throws JMSException {
            final MQXAConnectionFactory wmqConnectionFactory = new MQXAConnectionFactory();
            wmqConnectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            wmqConnectionFactory.setHostName(
                    "10.20.28.12");
            wmqConnectionFactory.setPort(
                    1414);
            wmqConnectionFactory.setQueueManager(
                    "VPA12");
            wmqConnectionFactory.setChannel(
                    "SMRV.TST");
//        wmqConnectionFactory.setShareConvAllowed(WMQ_SHARE_CONV_ALLOWED_YES); // share conncetions on the server side 
//        wmqConnectionFactory.setUseConnectionPooling(true);
            return wmqConnectionFactory;

        }

        @Bean(name = "atomikosTransactionManager",
                initMethod = "init",
                destroyMethod = "close")
        public UserTransactionManager getTransactionManager() {
            final UserTransactionManager userTransactionManager = new UserTransactionManager();
            userTransactionManager.setForceShutdown(true);
            return userTransactionManager;
        }

        @Bean(name = "atomikosUserTransaction")
        public UserTransaction getUserTransaction() throws SystemException {
            final UserTransactionImp userTransactionImp = new UserTransactionImp();
            userTransactionImp.setTransactionTimeout(2000);
            return userTransactionImp;
        }

        @Bean(name = "jtaTransactionManager")
        public JtaTransactionManager getJtaTransactionManager(UserTransactionManager transactionManager,
                @Qualifier("atomikosUserTransaction") UserTransaction userTransaction) {
            final JtaTransactionManager jta = new JtaTransactionManager();
            jta.setTransactionManager(transactionManager);
            jta.setUserTransaction(userTransaction);
            return jta;
        }

        @Bean(name = "atomikosConnectionFactoryBean",
                initMethod = "init",
                destroyMethod = "close")
        @Scope(value = SCOPE_PROTOTYPE)
        public AtomikosConnectionFactoryBean getAtomikosConnectionFactory(XAConnectionFactory connectionFactory,
                int poolSize,
                String uniqueResourceName) {
            final AtomikosConnectionFactoryBean cf = new AtomikosConnectionFactoryBean();
            cf.setPoolSize(poolSize);
            cf.setXaConnectionFactory(connectionFactory);
            cf.setUniqueResourceName(uniqueResourceName);

            return cf;
        }

        private AtomikosConnectionFactoryBean createConnectionFactory(BeanFactory beanFactory,
                XAConnectionFactory connectionFactory,
                int poolSize,
                String uniqueResourceName) {
            final AtomikosConnectionFactoryBean atomikosCF
                    = (AtomikosConnectionFactoryBean) beanFactory.getBean("atomikosConnectionFactoryBean",
                            connectionFactory,
                            poolSize,
                            uniqueResourceName);

            return atomikosCF;
        }
    }

    @Autowired
    private AbstractApplicationContext ac;

    private final int counter = 30;

    @Override
    protected void doPostSetup() throws Exception {
        super.doPostSetup();

        final EventNotifierSupport eventNotifier = new EventNotifierSupport() {

            @Override
            public void notify(EventObject event) throws Exception {
                if (event instanceof ExchangeSentEvent) {
                    ExchangeSentEvent sent = (ExchangeSentEvent) event;
                    System.out.println("Took " + sent.getTimeTaken() + " millis to send to: " + sent.getEndpoint());
                }
            }

            @Override
            public boolean isEnabled(EventObject event) {
                // we only want the sent events
                return event instanceof ExchangeSentEvent;
            }

            @Override
            protected void doStart() throws Exception {
                // noop
            }

            @Override
            protected void doStop() throws Exception {
                // noop
            }
        };

        context.getManagementStrategy().addEventNotifier(eventNotifier);
        eventNotifier.start();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /* Move messages from Websphere MQ to Active MQ using JTA/XA transactions. */
    @Test
    public void testReadAndStoreWMQ2AMQ() throws Exception {
        System.out.println("testReadAndStoreWMQ2AMQ");

        template.setDefaultEndpointUri("wmq:SMRV.TO.CORONA");

        System.out.println("Creating " + counter + " messages.");
        for (int i = 0; i < counter; i++) {
            template.sendBody(PAYLOAD);
        }
        System.out.println("Done creating messages.");

        final long millis = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(counter);
        context.addRoutes(new SpringRouteBuilder() {
            public void configure() throws Exception {
                from("wmqxa:SMRV.TO.CORONA")
                        .transacted()
                        .process(new Processor() {
                            public void process(Exchange exchange) throws
                            Exception {
                                latch.countDown();
                            }
                        })
                        .to("amqxa:queue:test");
            }
        });

        assertTrue(latch.await(5, TimeUnit.MINUTES));
        System.out.println("testReadAndStoreXWAMQ2AMQ consumed " + counter + " messages in " + (System.currentTimeMillis() - millis) + " ms.");
    }

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return ac;
    }
}
