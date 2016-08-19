package org.atlasapi.remotesite.lovefilm;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import com.google.common.io.ByteSource;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.atlasapi.remotesite.FetchException;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import com.google.common.base.Supplier;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;

public class S3LoveFilmDataSupplier implements LoveFilmDataSupplier {

    private static final String NO_DELIMITER = null;
    
    private final Supplier<S3Service> serviceSupplier;
    private final String bucketName;
    private final String folder;
    private final String fileName;

    public S3LoveFilmDataSupplier(Supplier<S3Service> serviceSupplier, String bucket, String folder, String fileName) {
        this.serviceSupplier = serviceSupplier;
        this.bucketName = bucket;
        this.folder = folder;
        this.fileName = fileName;
    }
    
    @Override
    public LoveFilmData getLatestData() throws FetchException {
        try {
            S3Service service = serviceSupplier.get();
            S3Object[] objects = service.listObjects(bucketName, folder, NO_DELIMITER);
            S3Object file = getFileforName(objects);
            if (file == null) {
                throw new FetchException(String.format("No data file in %s/%s", bucketName, folder));
            }

            ByteSource in = inputStreamFor(service, file);
            Charset charset = Charset.forName("windows-1252");

            return new LoveFilmData(in.asCharSource(charset));
        } catch (ServiceException e) {
            throw new FetchException(e.getMessage(), e);
        } 
    }

    private S3Object getFileforName(S3Object[] objects) {
        for (S3Object obj : objects) {
            String matchName = folder + '/' + fileName;
            if (matchName.equals(obj.getName())) {
                return obj;
            }
        }
        return null;
    }

    private ByteSource inputStreamFor(final S3Service service, S3Object object) {
        final String key = object.getKey();
        return new ByteSource() {

            @Override
            public InputStream openStream() throws IOException {
                try {
                    S3Object fullObject = service.getObject(bucketName, key);
                    return new BZip2CompressorInputStream(fullObject.getDataInputStream());
                } catch (ServiceException e) {
                    throw new IOException(e.getMessage(), e);
                }
            }
        };
    }
}
