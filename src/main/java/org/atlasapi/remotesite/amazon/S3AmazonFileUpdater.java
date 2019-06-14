package org.atlasapi.remotesite.amazon;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.atlasapi.remotesite.FetchException;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import com.google.common.base.Supplier;
import com.google.common.collect.Ordering;


public class S3AmazonFileUpdater implements AmazonFileUpdater {
    
    private final String bucketName;
    private final Supplier<S3Service> serviceSupplier;
    private final AmazonFileStore fileStore;
    private final Ordering<S3Object> fileOrdering;

    public S3AmazonFileUpdater(
            Supplier<S3Service> serviceSupplier,
            String bucketName,
            Ordering<S3Object> fileOrdering,
            AmazonFileStore fileStore
    ) {
        this.fileOrdering = checkNotNull(fileOrdering);
        this.serviceSupplier = checkNotNull(serviceSupplier);
        this.bucketName = checkNotNull(bucketName);
        this.fileStore = checkNotNull(fileStore);
    }

    @Override
    public void update() {
        try {
            S3Service service = serviceSupplier.get();
            S3Object[] objects = service.listObjects(bucketName);
            S3Object file = fileOrdering.max(Arrays.asList(objects));
            fileStore.save(file.getName(), inputStreamFor(service, file));
        } catch (NoSuchElementException e) {
            throw new FetchException("No matching file found", e);
        } catch (ServiceException e) {
            throw new FetchException(e.getMessage(), e);
        } catch (IOException e) {
            throw new FetchException(e.getMessage(), e);
        } 
    }

    private InputStream inputStreamFor(
            S3Service service,
            S3Object object
    ) throws ServiceException {
        S3Object fullObject = service.getObject(bucketName, object.getKey());
        return fullObject.getDataInputStream();
    }
}
