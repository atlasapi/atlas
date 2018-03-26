package org.atlasapi.remotesite.bbc.nitro;

import org.atlasapi.remotesite.bbc.nitro.v1.NitroClient;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroFormat;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroGenreGroup;

import java.util.List;

public class UnconfiguredNitroClient implements NitroClient {

    private static final NitroClient instance = new UnconfiguredNitroClient();

    public static NitroClient get() {
        return instance;
    }

    private UnconfiguredNitroClient() { }
    
    @Override
    public List<NitroFormat> formats(String pid) throws NitroException {
        throw unconfigured();
    }
    
    @Override
    public List<NitroGenreGroup> genres(String pid) throws NitroException {
        throw unconfigured();
    }

    private UnsupportedOperationException unconfigured() {
        return new UnsupportedOperationException("not configured");
    }
}
