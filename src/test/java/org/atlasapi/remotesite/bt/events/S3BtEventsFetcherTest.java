package org.atlasapi.remotesite.bt.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

import org.jets3t.service.S3Service;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;


public class S3BtEventsFetcherTest {

    private static final String MOTO_GP_FILENAME = "motogpfile";
    private static final BtSportType SPORT = BtSportType.MOTO_GP;
    private S3Service s3Service = Mockito.mock(S3Service.class);
    private Map<BtSportType, String> fileNames = ImmutableMap.of(
            SPORT, MOTO_GP_FILENAME
    );
    private String bucketName = "bucket";
    private final S3BtEventsFetcher fetcher = new S3BtEventsFetcher(s3Service, fileNames, bucketName);
    
    @Test
    public void testConversionToBtDataFormat() throws IOException {
        InputStream stream = streamFromFile("moto_gp_feed.json");
        Optional<BtEventsData> parsed = fetcher.extractData(stream);
        
        BtEventsData data = parsed.get();
        
        assertTrue(Iterables.isEmpty(data.teams()));
        
        assertEquals(19, Iterables.size(data.matches()));
    }
    
    private InputStream streamFromFile(String filename) throws IOException {
        URL testFile = Resources.getResource(getClass(), filename);
        return testFile.openStream();
    }

}
