package io.mats3.amq;

import java.io.IOException;

import org.apache.jasper.servlet.JasperInitializer;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class MatsAmqMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MatsAmqMonitorApplication.class, args);
    }

    @Bean
    public ServletWebServerFactory servletContainer() {
        JettyServletWebServerFactory jetty = new JettyServletWebServerFactory("", 9000);
        jetty.addServerCustomizers(server -> {
            String warFileMagicStringReference = "CLASSPATH AMQ WEB CONSOLE WAR FILE";
            // Override WebAppContext to be able to load AMQ Web Console war file from classpath.
            WebAppContext amqWebConsoleWebAppContext = new WebAppContext(warFileMagicStringReference, "/webconsole") {
                /**
                 * Hack-override the Resource creation if it is the AMQ Web Console War that is requested. Very strange
                 * that I couldn't find a more natural way to stick in a specific Jetty Resource as war file to load
                 * (the "classpath:" URL prefix is not standard Java, and the hacks available to fix this are way more
                 * intrusive that this hack. At the same time Jetty does have both a URLResource and ClassPath
                 * implementation, but which is out of reach when we can only specify a String as the "war" argument).
                 */
                @Override
                public Resource newResource(String urlOrPath) throws IOException {
                    if (urlOrPath == warFileMagicStringReference) {
                        return Resource.newClassPathResource("activemq-web-console.war");
                    }
                    return Resource.newResource(urlOrPath);
                }
            };
            HandlerCollection handlerCollection = new HandlerCollection();
            handlerCollection.addHandler(amqWebConsoleWebAppContext);
            handlerCollection.addHandler(server.getHandler());
            server.setHandler(handlerCollection);

            // Let the Web Console's classloader use parent first loading, to share e.g. SLFJ4 and Logback..
            amqWebConsoleWebAppContext.setParentLoaderPriority(true);

            /*
             * Since the Servlet Container is initialized by Spring Boot (JettyEmbeddedServletContainerFactory), the
             * JasperInitializer (which is a ServletContainerInitializer) won't be run. Therefore we force it to be run
             * via Jetty's DI-container.
             */
            amqWebConsoleWebAppContext.addManaged(new MatsJasperInitializer(amqWebConsoleWebAppContext));
        });
        return jetty;
    }

    /**
     * Inspiration taken from {@link org.springframework.boot.context.embedded.jetty.JasperInitializer}.
     *
     * Notice that I've removed the invocation of setting URL's StreamHandlerFactory to handle WAR files (
     * <code>URL.setURLStreamHandlerFactory(new WarUrlStreamHandlerFactory());</code>), as this evidently wasn't needed
     * for the usage of embedding AMQ Web Console.
     */
    private static class MatsJasperInitializer extends AbstractLifeCycle {
        private final WebAppContext context;

        MatsJasperInitializer(WebAppContext context) {
            this.context = context;
        }

        @Override
        protected void doStart() throws Exception {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(this.context.getClassLoader());
                try {
                    context.getServletContext().setExtendedListenerTypes(true);
                    new JasperInitializer().onStartup(null, this.context.getServletContext());
                }
                finally {
                    context.getServletContext().setExtendedListenerTypes(false);
                }
            }
            finally {
                Thread.currentThread().setContextClassLoader(classLoader);
            }
        }
    }
}
