
package io.smallrye.reactive.messaging.amqp;

import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class AmqpBroker {

    static EmbeddedActiveMQ server;
    static AmqpBroker instance;

    private String brokerXmlName;

    public Map<String, String> start(int port) {
        instance = this;
        try {
            server = new EmbeddedActiveMQ();
            if (brokerXmlName == null) {
                Configuration config = new ConfigurationImpl();
                config.setPagingDirectory("target/data/paging");
                config.setBindingsDirectory("target/data/bindings");
                config.setJournalDirectory("target/data/journal");
                config.setLargeMessagesDirectory("target/data/large-messages");
                config.addAcceptorConfiguration("in-vm", "vm://0");
                config.addAcceptorConfiguration("tcp", "tcp://127.0.0.1:" + port);
                server.setConfiguration(config);
            } else {
                server.setConfigResourcePath(brokerXmlName);
            }
            server.setSecurityManager(new ActiveMQSecurityManager() {
                @Override
                public boolean validateUser(String username, String password) {
                    return username.equalsIgnoreCase("artemis") && password.equalsIgnoreCase("artemis");
                }

                @Override
                public boolean validateUserAndRole(String username, String password,
                        Set<Role> roles,
                        CheckType checkType) {
                    return username.equalsIgnoreCase("artemis") && password.equalsIgnoreCase("artemis");
                }
            });
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        await().until(() -> server.getActiveMQServer() != null
                && server.getActiveMQServer().isActive()
                && server.getActiveMQServer().getConnectorsService().isStarted());
        return Collections.emptyMap();
    }

    public void stop() {
        try {
            if (server != null) {
                server.stop();
            }
            instance = null;
            brokerXmlName = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public EmbeddedActiveMQ getServer() {
        return server;
    }

    public void setConfigResourcePath(String brokerXmlName) {
        this.brokerXmlName = brokerXmlName;
    }
}
