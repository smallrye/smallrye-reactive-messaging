package quickstart;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainerInitializer;

@ApplicationScoped
public class Main {

    public static void main(String[] args) {
        SeContainerInitializer.newInstance().initialize();
    }

}
