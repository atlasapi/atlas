package org.atlasapi.remotesite.amazon;

import java.io.IOException;
import java.io.InputStream;

public interface AmazonFileStore {
    
    void save(String fileName, InputStream dataStream) throws IOException;
    
    InputStream getLatestData() throws NoDataException, IOException;
}
