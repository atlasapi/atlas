package org.atlasapi.remotesite.amazonunbox;

import java.io.IOException;
import java.io.InputStream;

public interface AmazonUnboxFileStore {
    
    void save(InputStream dataStream) throws IOException;
    
    InputStream getLatestData() throws NoDataException;
}
