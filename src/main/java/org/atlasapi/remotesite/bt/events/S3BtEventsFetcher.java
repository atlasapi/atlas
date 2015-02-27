package org.atlasapi.remotesite.bt.events;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

import org.atlasapi.remotesite.FetchException;
import org.atlasapi.remotesite.bt.events.model.BtEvent;
import org.atlasapi.remotesite.bt.events.model.BtEventsFeed;
import org.atlasapi.remotesite.events.EventsData;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;


public class S3BtEventsFetcher implements BtEventsFetcher {

    private static final String NO_DELIMITER = null;
    
    private final Gson gson = new GsonBuilder().create();
    private final S3Service s3Service;
    private final Map<BtSportType, String> fileNames;
    private final String bucketName;
    
    public S3BtEventsFetcher(S3Service s3Service, Map<BtSportType, String> fileNames, 
            String bucketName) {
        this.s3Service = checkNotNull(s3Service);
        this.fileNames = checkNotNull(fileNames);
        this.bucketName = checkNotNull(bucketName);
    }
    
    @Override
    public Optional<? extends EventsData<BtTeam, BtEvent>> fetch(BtSportType sport) throws FetchException {
        try {
            String fileName = fileNames.get(sport);
            if (fileName == null) {
                return Optional.absent();
            }
            S3Object[] objects = s3Service.listObjects(bucketName, "", NO_DELIMITER);
            S3Object file = getFileforName(objects, fileName);
            if (file == null) {
                throw new FetchException(String.format("No data file %s in %s", fileName, bucketName));
            }
            InputStream in = inputStreamFor(s3Service, file);
            return extractData(in);
        } catch (ServiceException | JsonSyntaxException | JsonIOException | IOException e) {
            throw new FetchException(e.getMessage(), e);
        } 
    }
    
    @Override
    public Set<BtSportType> sports() {
        return fileNames.keySet();
    }
    
    public Optional<BtEventsData> extractData(InputStream input) {
        BtEventsFeed eventsFeed = gson.fromJson(new InputStreamReader(input), BtEventsFeed.class);
        return Optional.of(new BtEventsData(eventsFeed.response().docs()));
    }
    
    private S3Object getFileforName(S3Object[] objects, String fileName) {
        for (S3Object obj : objects) {
            if (fileName.equals(obj.getName())) {
                return obj;
            }
        }
        return null;
    }

    private InputStream inputStreamFor(final S3Service service, S3Object object) throws IOException {
        final String key = object.getKey();
        try {
            S3Object fullObject = service.getObject(bucketName, key);
            return fullObject.getDataInputStream();
        } catch (ServiceException e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
