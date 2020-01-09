package com.stolsvik.mats.websocket;

import java.io.IOException;
import java.net.URL;
import java.security.Principal;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.jms.ConnectionFactory;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.server.ServerContainer;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.AbstractNetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletHolder;
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
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEndpoint;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward;
import com.stolsvik.mats.websocket.impl.ClusterStoreAndForward_SQL;
import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer;

import ch.qos.logback.core.CoreConstants;

/**
 * @author Endre Stølsvik 2019-11-21 21:07 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsTestWebsocketServer {

    private static final String CONTEXT_ATTRIBUTE_PORTNUMBER = "ServerPortNumber";

    private static final String COMMON_AMQ_NAME = "CommonAMQ";

    private static final Logger log = LoggerFactory.getLogger(MatsTestWebsocketServer.class);

    @WebListener
    public static class SCL_Endre implements ServletContextListener {

        private MatsSocketServer _matsSocketServer;
        private MatsFactory _matsFactory;

        @Override
        public void contextInitialized(ServletContextEvent sce) {
            log.info("ServletContextListener.contextInitialized(...): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());

            // :: H2 DataBase
            JdbcDataSource h2Ds = new JdbcDataSource();
            h2Ds.setURL("jdbc:h2:~/temp/matsproject_dev_h2database/matssocket_dev;AUTO_SERVER=TRUE");
            JdbcConnectionPool dataSource = JdbcConnectionPool.create(h2Ds);

            // :: ActiveMQ and MatsFactory
            ConnectionFactory connectionFactory = MatsLocalVmActiveMq.createConnectionFactory(COMMON_AMQ_NAME);
            MatsSerializer_DefaultJson matsSerializer = new MatsSerializer_DefaultJson();
            _matsFactory = JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(
                    this.getClass().getSimpleName(), "*testing*",
                    new JmsMatsJmsSessionHandler_Pooling((s) -> connectionFactory.createConnection()),
                    matsSerializer);

            // Configure the MatsFactory for testing (remember, two instances on same box)
            // .. Concurrency of only 1
            _matsFactory.getFactoryConfig().setConcurrency(1);
            // .. Use port number of current server as postfix for name of MatsFactory, and of nodename
            Integer portNumber = (Integer) sce.getServletContext().getAttribute(CONTEXT_ATTRIBUTE_PORTNUMBER);
            _matsFactory.getFactoryConfig().setName("MF_Server_" + portNumber);
            _matsFactory.getFactoryConfig().setNodename("EndreBox_" + portNumber);

            // :: Make MatsEndpoint
            _matsFactory.single("Test.single", MatsDataTO.class, MatsDataTO.class, (processContext, incomingDto) -> {
                return new MatsDataTO(incomingDto.number, incomingDto.string + ":FromSimple", incomingDto.multiplier);
            });

            // :: Create MatsSocketServer
            // Cluster-stuff for the MatsSocketServer
            // ClusterStoreAndForward_DummySingleNode csaf = new ClusterStoreAndForward_DummySingleNode(matsFactory
            // .getFactoryConfig().getNodename());
            ClusterStoreAndForward_SQL csaf = ClusterStoreAndForward_SQL.create(dataSource, _matsFactory
                    .getFactoryConfig().getNodename());
            // Make a Dummy Authentication plugin
            Function<String, Principal> authToPrincipalFunction = authHeader -> {
                log.info("Resolving Authorization header to principal for header [" + authHeader + "].");
                long expires = Long.parseLong(authHeader.substring(authHeader.indexOf(':') + 1));
                if (expires < System.currentTimeMillis()) {
                    throw new IllegalStateException("This DummyAuth is too old.");
                }
                return new Principal() {
                    @Override
                    public String getName() {
                        return "Mr. Dummy Auth";
                    }

                    @Override
                    public String toString() {
                        return "DummyPrincipal:" + authHeader;
                    }
                };
            };
            // Create the MatsSocketServer
            _matsSocketServer = getMatsSocketServer(sce, _matsFactory, csaf, authToPrincipalFunction);
            sce.getServletContext().setAttribute(MatsSocketServer.class.getName(), _matsSocketServer);
            MatsSocketServer matsSocketServer = (MatsSocketServer) sce.getServletContext().getAttribute(
                    MatsSocketServer.class.getName());
            log.info("EndreXY: servletContext MatsSocketServer:" + matsSocketServer);

            // :: Make MatsSocketEndpoint
            MatsSocketEndpoint<MatsSocketRequestDto, MatsDataTO, MatsDataTO, MatsSocketReplyDto> matsSocketEndpoint = _matsSocketServer
                    .matsSocketEndpoint("Test.single",
                            MatsSocketRequestDto.class, MatsDataTO.class, MatsDataTO.class, MatsSocketReplyDto.class,
                            (ctx, principal, msIncoming) -> {
                                log.info("Got MatsSocket request on MatsSocket EndpointId: "
                                        + ctx.getMatsSocketEndpointId());
                                log.info(" \\- Authorization: " + ctx.getAuthorization());
                                log.info(" \\- Principal:     " + ctx.getPrincipal());
                                log.info(" \\- Message:       " + msIncoming);
                                ctx.forwardCustom(new MatsDataTO(msIncoming.number, msIncoming.string),
                                        msg -> {
                                            msg.to(ctx.getMatsSocketEndpointId())
                                                    .interactive()
                                                    .nonPersistent()
                                                    .setTraceProperty("requestTimestamp", msIncoming.requestTimestamp);
                                        });
                            });
            matsSocketEndpoint.replyAdapter((ctx, matsReply) -> {
                log.info("Adapting message: " + matsReply);
                return new MatsSocketReplyDto(matsReply.string.length(), matsReply.number,
                        ctx.getMatsContext().getTraceProperty("requestTimestamp", Long.class));
            });
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            log.info("ServletContextListener.contextDestroyed(..): " + sce);
            log.info("  \\- ServletContext: " + sce.getServletContext());
            _matsSocketServer.shutdown();
            _matsFactory.stop(1000);
        }
    }

    private static MatsSocketServer getMatsSocketServer(ServletContextEvent sce, MatsFactory matsFactory,
            ClusterStoreAndForward clusterStoreAndForward, Function<String, Principal> authToPrincipalFunction) {
        Object serverContainerAttrib = sce.getServletContext().getAttribute(ServerContainer.class.getName());
        if (!(serverContainerAttrib instanceof ServerContainer)) {
            throw new AssertionError("Did not find '" + ServerContainer.class.getName() + "' object"
                    + " in ServletContext, but [" + serverContainerAttrib + "].");
        }

        ServerContainer wsServerContainer = (ServerContainer) serverContainerAttrib;
        return DefaultMatsSocketServer.createMatsSocketServer(
                wsServerContainer, matsFactory, clusterStoreAndForward, authToPrincipalFunction);
    }

    @WebServlet("/test")
    public static class TestServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.getWriter().println("Testing Servlet");
        }
    }

    @WebServlet("/shutdown")
    public static class ShutdownServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.getWriter().println("Shutting down");

            // Shut down the process
            ForkJoinPool.commonPool().submit(() -> System.exit(0));
        }
    }

    public static String id(Object x) {
        return x.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(x));
    }

    public static Server createServer(int port) {
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setBaseResource(Resource.newClassPathResource("webapp"));
        webAppContext.setThrowUnavailableOnStartupException(true);

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
        URL classes = MatsTestWebsocketServer.class.getProtectionDomain().getCodeSource().getLocation();
        // Set this location to be scanned.
        webAppContext.getMetaData().setWebInfClassesDirs(Collections.singletonList(Resource.newResource(classes)));

        // Create the actual Jetty Server
        Server server = new Server(port);

        // Add StatisticsHandler (to enable graceful shutdown), put in the WebApp Context
        StatisticsHandler stats = new StatisticsHandler();
        stats.setHandler(webAppContext);
        server.setHandler(stats);

        // Add a Jetty Lifecycle Listener to cleanly shut down the MatsSocketServer.
        server.addLifeCycleListener(new AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStopping(LifeCycle event) {
                log.info("XXXX lifeCycleStopping for " + port + ", event:" + event + ", WebAppContext:" + webAppContext
                        + ", servletContext:" + webAppContext.getServletContext());
                MatsSocketServer matsSocketServer = (MatsSocketServer) webAppContext.getServletContext().getAttribute(
                        MatsSocketServer.class.getName());
                log.info("MatsSocketServer instance:" + matsSocketServer);
                matsSocketServer.shutdown();
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

        // Create common AMQ
        MatsLocalVmActiveMq inVmActiveMq = MatsLocalVmActiveMq.createInVmActiveMq(COMMON_AMQ_NAME);

        // Read in the server count as an argument, or assume 2
        int serverCount = (args.length > 0) ? Integer.parseInt(args[0]) : 2;
        // Read in start port to count up from, defaulting to 8080
        int nextPort = (args.length > 1) ? Integer.parseInt(args[0]) : 8080;

        // Start the desired number of servers
        Server[] servers = new Server[serverCount];
        for (int i = 0; i < servers.length; i++) {
            int serverId = i + 1;

            // Keep looping until we have found a free port that the server was able to start on
            while (true) {
                int port = nextPort;
                servers[i] = createServer(port);
                log.info("######### Starting server [" + serverId + "] on [" + port + "]");

                // Add a life cycle hook to log when the server has started
                servers[i].addLifeCycleListener(new AbstractLifeCycleListener() {
                    @Override
                    public void lifeCycleStarted(LifeCycle event) {
                        log.info("######### Started server " + serverId + " on port " + port);
                        log.info("WS_URL: ws://localhost:" + port + DefaultMatsSocketServer.PATH);
                    }
                });

                // Try and start the server on the port we set. If this fails, we will increment the port number
                // and try again.
                try {
                    servers[i].start();
                    break;
                }
                catch (Exception e) {
                    log.info("######### Failed to start server [" + serverId + "] on [" + port + "], trying next port.", e);
                }
                finally {
                    // Always increment the port number
                    nextPort++;
                }
            }
        }


    }
}
