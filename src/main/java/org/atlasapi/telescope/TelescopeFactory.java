package org.atlasapi.telescope;

import com.metabroadcast.columbus.telescope.api.Environment;
import com.metabroadcast.columbus.telescope.api.Process;

/**
 * Creates proxies to telescopeClients that can be used for reporting to telescope.
 * <p>
 * If you need to extend this class to accommodate more Processes (i.e. add more owl ingesters),
 * extend the {@link IngesterName} enum accordingly.
 * <p>
 * Created by andreas on 07/07/2017.
 */
public class TelescopeFactory {


    /**
     * Be advised that making multiple telescope clients with the same name and using them concurrently is likely to cause
     * errors, unspecified bugs, losing your bananas and generally you are gonna have a bad day.
     */
    public static TelescopeProxy make(IngesterName ingesterName) {
        Process process = getProcess(ingesterName);
        TelescopeProxy telescopeProxy = new TelescopeProxy(process);

        return telescopeProxy;
    }


    //create and return a telescope.api.Process.
    private static Process getProcess(IngesterName name) {

        return com.metabroadcast.columbus.telescope.api.Process.create(
                name.getIngesterKey(),
                name.getIngesterName(),
                Environment.valueOf(TelescopeConfiguration.ENVIRONMENT));
    }

    /**
     * Holds the pairs of Ingester Keys-Names used by atlas to report to telescope.
     */
    public enum IngesterName {
        BBC_NITRO("bbc-nitro", "BBC Nitro Ingester (Owl)");

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
