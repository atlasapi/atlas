package org.atlasapi.remotesite.bbc.nitro.v1;

import org.atlasapi.remotesite.bbc.nitro.NitroException;

import java.util.List;


public interface NitroClient {

    List<NitroGenreGroup> genres(String pid) throws NitroException;
    
    List<NitroFormat> formats(String pid) throws NitroException;
    
}
