package io.smallrye.reactive.messaging.amqp.ssl;

import static io.smallrye.reactive.messaging.amqp.ssl.KeyStores.SERVER_KS_PASSWORD;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.smallrye.reactive.messaging.amqp.AmqpSourceCDIConfigTest;

public class SSLBrokerConfigUtil {
    static String createSecuredBrokerXml() throws IOException, URISyntaxException {
        Path path = Files.createTempFile(Paths.get("./target/test-classes/ssl"), "broker-ssl", ".xml");
        List<String> contents = Files.readAllLines(
                Paths.get(AmqpSourceCDIConfigTest.class.getResource("/ssl/broker-ssl.xml").toURI()));
        List<String> replaced = new ArrayList<>();
        Path keystore = Paths.get(KeyStores.getServerKsUrl().toURI());
        for (String input : contents) {
            input = input.replace("${keystore}", keystore.toString());
            input = input.replace("${keystorepass}", SERVER_KS_PASSWORD);
            replaced.add(input);
        }
        Files.write(path, replaced);
        return "ssl/" + path.getFileName().toString();
    }
}
