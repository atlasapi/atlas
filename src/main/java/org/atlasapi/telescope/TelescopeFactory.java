package org.atlasapi.telescope;

import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Process;
import com.metabroadcast.common.properties.Configurer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates proxies to telescopeClients that can be used for reporting to telescope.
 * <p>
 * If you need to extend this class to accommodate more Processes (i.e. add more owl ingesters),
 * extend the {@link IngesterName} enum accordingly.
 */
public class TelescopeFactory {

    public static final String TELESCOPE_HOST = Configurer.get("telescope.host").get();
    public static final String ENVIRONMENT = Configurer.get("telescope.environment").get();
    private static final Logger log = LoggerFactory.getLogger(TelescopeFactory.class);

    /**
     * This factory will always give you a telescope (never null). If there are initialization
     * errors the telescope you will get might be unable to report.
     */
    public static TelescopeProxy make(IngesterName ingesterName) {
        Process process = getProcess(ingesterName);
        TelescopeProxy telescopeProxy = new TelescopeProxy(process);

        return telescopeProxy;
    }

    //create and return a telescope.api.Process.
    private static Process getProcess(IngesterName name) {
        Environment environment;
        try {
            environment = Environment.valueOf(ENVIRONMENT);
        } catch (IllegalArgumentException e) {
            //add stage as the default environment, which is better than crashing
            environment = Environment.STAGE;
            log.error(
                    "Could not find a telescope environment with the given name, name={}. Falling back to STAGE.",
                    ENVIRONMENT,
                    e
            );
        }

        return Process.create(name.getIngesterKey(), name.getIngesterName(), environment);
    }

    /**
     * Holds the pairs of Ingester Keys-Names used by atlas to report to telescope.
     */
    public enum IngesterName {
        BBC_NITRO("bbc-nitro-ingester", "BBC Nitro Ingester (Owl)");

        String ingesterKey;
        String ingesterName;

        IngesterName(String ingesterKey, String ingesterName) {
            this.ingesterKey = ingesterKey;
            this.ingesterName = ingesterName;
        }

        public String getIngesterKey() {
            return ingesterKey;
        }

        public String getIngesterName() {
            return ingesterName;
        }
    }

}
