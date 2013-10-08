package org.atlasapi.remotesite.amazonunbox;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.atlasapi.remotesite.FetchException;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import com.google.common.base.Supplier;


public class S3AmazonUnboxFileUpdater implements AmazonUnboxFileUpdater {
    
    private final String bucketName;
    private final Supplier<S3Service> serviceSupplier;
    private final String fileName;
    private final AmazonUnboxFileStore fileStore;

    public S3AmazonUnboxFileUpdater(Supplier<S3Service> serviceSupplier, String bucketName, String fileName, AmazonUnboxFileStore fileStore) {
        this.serviceSupplier = checkNotNull(serviceSupplier);
        this.bucketName = checkNotNull(bucketName);
        this.fileName = checkNotNull(fileName);
        this.fileStore = checkNotNull(fileStore);
    }

    @Override
    public void update() {
        try {
            S3Service service = serviceSupplier.get();
            S3Object[] objects = service.listObjects(bucketName);
            S3Object file = getFileforName(objects);
            if (file == null) {
                throw new FetchException(String.format("No data file in %s", bucketName));
            }
            fileStore.save(inputStreamFor(service, file));
        } catch (ServiceException e) {
            throw new FetchException(e.getMessage(), e);
        } catch (IOException e) {
            throw new FetchException(e.getMessage(), e);
        } 
    }

    private S3Object getFileforName(S3Object[] objects) {
        for (S3Object obj : objects) {
            if (fileName.equals(obj.getName())) {
                return obj;
            }
        }
        return null;
    }

    private InputStream inputStreamFor(S3Service service, S3Object object) throws ServiceException {
        S3Object fullObject = service.getObject(bucketName, object.getKey());
        return fullObject.getDataInputStream();
    }
}
