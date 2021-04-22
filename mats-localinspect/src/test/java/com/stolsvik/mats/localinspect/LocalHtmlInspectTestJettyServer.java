package com.stolsvik.mats.localinspect;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;

import javax.jms.ConnectionFactory;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.component.AbstractLifeCycle.AbstractLifeCycleListener;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebXmlConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator.KeepTrace;
import com.stolsvik.mats.api.intercept.MatsInterceptableMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.localinspect.SetupTestMatsEndpoints.DataTO;
import com.stolsvik.mats.localinspect.SetupTestMatsEndpoints.StateTO;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.json.MatsSerializerJson;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

import ch.qos.logback.core.CoreConstants;

/**
 * Test server for the HTML interface generation. Pulling up a bunch of endpoints, and having a traffic generator to put
 * them to some use.
 *
 * @author Endre Stølsvik 2021-03-25 12:29 - http://stolsvik.com/, endre@stolsvik.com
 */
public class LocalHtmlInspectTestJettyServer {

    private static final String CONTEXT_ATTRIBUTE_PORTNUMBER = "ServerPortNumber";

    private static final String COMMON_AMQ_NAME = "CommonAMQ";

    private static final Logger log = LoggerFactory.getLogger(LocalHtmlInspectTestJettyServer.class);

    @WebListener
    public static class SCL_Endre implements ServletContextListener {

        private MatsInterceptableMatsFactory _matsFactory;

        @Override
        public void contextInitialized(ServletContextEvent sce) {
            log.info("ServletContextListener.contextInitialized(...): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());

            // ## Create DataSource using H2
            JdbcDataSource h2Ds = new JdbcDataSource();
            h2Ds.setURL("jdbc:h2:~/temp/matsproject_dev_h2database/matssocket_dev"
                    + ";AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE");
            JdbcConnectionPool dataSource = JdbcConnectionPool.create(h2Ds);
            dataSource.setMaxConnections(5);

            // ## Create MatsFactory
            // ActiveMQ ConnectionFactory
            ConnectionFactory connectionFactory = MatsLocalVmActiveMq.createConnectionFactory(COMMON_AMQ_NAME);
            // MatsSerializer
            MatsSerializer<String> matsSerializer = MatsSerializerJson.create();
            // Create the MatsFactory
            _matsFactory = JmsMatsFactory.createMatsFactory_JmsAndJdbcTransactions(
                    LocalHtmlInspectTestJettyServer.class.getSimpleName(), "*testing*",
                    JmsMatsJmsSessionHandler_Pooling.create(connectionFactory),
                    dataSource,
                    matsSerializer);

            // Hold start
            _matsFactory.holdEndpointsUntilFactoryIsStarted();

            // Install the stats keeper interceptor
            LocalStatsMatsInterceptor.install(_matsFactory);

            // Create the HTML interfaces, and store them in the ServletContext attributes, for the rendering Servlet.
            LocalHtmlInspectForMatsFactory interface1 = LocalHtmlInspectForMatsFactory.create(_matsFactory);
            LocalHtmlInspectForMatsFactory interface2 = LocalHtmlInspectForMatsFactory.create(_matsFactory);
            sce.getServletContext().setAttribute("interface1", interface1);
            sce.getServletContext().setAttribute("interface2", interface2);

            // Configure the MatsFactory for testing
            _matsFactory.getFactoryConfig().setConcurrency(SetupTestMatsEndpoints.BASE_CONCURRENCY);
            // .. Use port number of current server as postfix for name of MatsFactory, and of nodename
            Integer portNumber = (Integer) sce.getServletContext().getAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER);
            _matsFactory.getFactoryConfig().setName(LocalHtmlInspectTestJettyServer.class.getSimpleName()
                    + "_" + portNumber);
            _matsFactory.getFactoryConfig().setNodename("EndreBox_" + portNumber);
            // Put it in ServletContext, for servlet to get
            sce.getServletContext().setAttribute(JmsMatsFactory.class.getName(), _matsFactory);

            SetupTestMatsEndpoints.setupMatsAndMatsSocketEndpoints(_matsFactory);

            _matsFactory.start();
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            log.info("ServletContextListener.contextDestroyed(..): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());
            _matsFactory.stop(5000);
        }
    }

    /**
     * RenderingServlet
     */
    @WebServlet("/")
    public static class RootServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.setContentType("text/html; charset=utf-8");
            LocalHtmlInspectForMatsFactory interface1 = (LocalHtmlInspectForMatsFactory) req.getServletContext()
                    .getAttribute("interface1");
            LocalHtmlInspectForMatsFactory interface2 = (LocalHtmlInspectForMatsFactory) req.getServletContext()
                    .getAttribute("interface2");

            PrintWriter out = resp.getWriter();
            out.println("<html>");
            out.println("  <head>");
            out.println("  </head>");
            out.println("  <body>");
            out.println("    <style>");
            interface1.getStyleSheet(out); // Include just once, use the first.
            out.println("    </style>");
            out.println("    <script>");
            interface1.getJavaScript(out); // Include just once, use the first.
            out.println("    </script>");
            out.println("    <h1>Test h1</h1>");
            out.println("    Endre tester h1");

            out.println("    <h2>Test h2</h2>");
            out.println("    Endre tester h2");

            out.println("    <h3>Test h3</h3>");
            out.println("    Endre tester h3<br /><br />");

            out.println("    <a href=\"sendRequest\">Send request</a> - to initialize Initiator"
                    + " and get some traffic.<br /><br />");

            interface1.createFactoryReport(out, true, true, true);
            interface2.createFactoryReport(out, true, true, true);

            out.println("  </body>");
            out.println("</html>");
        }
    }

    /**
     * SEND A REQUEST THROUGH THE MATS SERVICE.
     */
    @WebServlet("/sendRequest")
    public static class SendRequestServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            log.info("Sending request ..");
            resp.getWriter().println("Sending request ..");
            MatsFactory matsFactory = (MatsFactory) req.getServletContext().getAttribute(JmsMatsFactory.class
                    .getName());

            StateTO sto = new StateTO(420, 420.024);
            DataTO dto = new DataTO(42, "TheAnswer");
            matsFactory.getDefaultInitiator().initiateUnchecked(
                    (msg) -> {
                        for (int i = 0; i < 1000; i++) {
                            msg.traceId("traceId" + i)
                                    .keepTrace(KeepTrace.MINIMAL)
                                    .nonPersistent()
                                    .from("LocalInterfaceTest.initiator")
                                    .to(SetupTestMatsEndpoints.SERVICE)
                                    .replyTo(SetupTestMatsEndpoints.TERMINATOR, sto)
                                    .request(dto);
                        }
                    });
            resp.getWriter().println(".. Request sent.");
        }
    }

    /**
     * Servlet to shut down this JVM (<code>System.exit(0)</code>).
     */
    @WebServlet("/shutdown")
    public static class ShutdownServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.getWriter().println("Shutting down");

            // Shut down the process
            ForkJoinPool.commonPool().submit(() -> System.exit(0));
        }
    }

    public static Server createServer(int port) {
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setBaseResource(Resource.newClassPathResource("webapp"));
        // If any problems starting context, then let exception through so that we can exit.
        webAppContext.setThrowUnavailableOnStartupException(true);
        // Store the port number this server shall run under in the ServletContext.
        webAppContext.getServletContext().setAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER, port);

        // Override the default configurations, stripping down and adding AnnotationConfiguration.
        // https://www.eclipse.org/jetty/documentation/9.4.x/configuring-webapps.html
        // Note: The default resides in WebAppContext.DEFAULT_CONFIGURATION_CLASSES
        webAppContext.setConfigurations(new Configuration[] {
                // new WebInfConfiguration(),
                new WebXmlConfiguration(), // Evidently adds the DefaultServlet, as otherwise no read of "/webapp/"
                // new MetaInfConfiguration(),
                // new FragmentConfiguration(),
                new AnnotationConfiguration() // Adds Servlet annotation processing.
        });

        // :: Get Jetty to Scan project classes too: https://stackoverflow.com/a/26220672/39334
        // Find location for current classes
        URL classesLocation = LocalHtmlInspectTestJettyServer.class.getProtectionDomain().getCodeSource()
                .getLocation();
        // Set this location to be scanned.
        webAppContext.getMetaData().setWebInfClassesDirs(Collections.singletonList(Resource.newResource(
                classesLocation)));

        // Create the actual Jetty Server
        Server server = new Server(port);

        // Add StatisticsHandler (to enable graceful shutdown), put in the WebApp Context
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(webAppContext);
        server.setHandler(stats);

        // Add a Jetty Lifecycle Listener to cleanly shut down stuff
        server.addLifeCycleListener(new AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStarted(LifeCycle event) {
                log.info("######### Started server on port " + port);
            }

            @Override
            public void lifeCycleStopping(LifeCycle event) {
                log.info("===== STOP! ===========================================");
            }
        });

        // :: Graceful shutdown
        server.setStopTimeout(1000);
        server.setStopAtShutdown(true);
        return server;
    }

    public static void main(String... args) throws Exception {
        // Turn off LogBack's absurd SCI
        System.setProperty(CoreConstants.DISABLE_SERVLET_CONTAINER_INITIALIZER_KEY, "true");

        // Create common AMQ server
        MatsLocalVmActiveMq.createInVmActiveMq(COMMON_AMQ_NAME);

        Server server = createServer(8080);
        server.start();
    }
}
