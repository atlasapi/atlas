package org.atlasapi.equiv.results.persistence;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.atlasapi.equiv.S3Processor;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;

import java.io.*;
import java.util.List;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;

/**
 * Filesystem-based store for equivalence results. New files will be stored in a
 * hashed directory structure to avoid a single directory with too many files in it, but
 * an attempt will also be made to read directly from the root directory for old results.
 */
public class S3EquivalenceResultStore implements EquivalenceResultStore {

    private StoredEquivalenceResultsTranslator translator = new StoredEquivalenceResultsTranslator();
    private final File baseDirectory;
    private final S3Processor s3Processor;

    public S3EquivalenceResultStore(File directory, String s3Access, String s3Secret, String s3Bucket) {
        if(!directory.isDirectory()) {
            throw new IllegalArgumentException("Must be a directory");
        }
        if(!directory.exists()) {
            throw new IllegalArgumentException("Directory does not exist");
        }
        this.baseDirectory = directory;
        this.s3Processor = S3Processor.create(s3Access, s3Secret, s3Bucket);
    }
    
    @Override
    public <T extends Content> StoredEquivalenceResults store(
            EquivalenceResults<T> results) {
        StoredEquivalenceResults storedEquivalenceResults = translator.toStoredEquivalenceResults(results);
        String filename = filenameFor(results.subject().getCanonicalUri()) + ".gz";
        try {
            File tempFile = File.createTempFile(filename, null);
            try (FileOutputStream fos = new FileOutputStream(tempFile);
                 GZIPOutputStream gos = new GZIPOutputStream(fos);
                 ObjectOutputStream os = new ObjectOutputStream(gos)) {
                os.writeObject(storedEquivalenceResults);
            }
            s3Processor.uploadFile(filename, tempFile);
            tempFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return storedEquivalenceResults;
    }

    @Override
    public StoredEquivalenceResults forId(String canonicalUri) {
        String filename = filenameFor(canonicalUri) + ".gz";

        Optional<StoredEquivalenceResults> resultInHashedDirectory = s3Processor.getStoredEquivalenceResults(filename);
        return resultInHashedDirectory.orElse(null);
    }

    @Override
    public List<StoredEquivalenceResults> forIds(Iterable<String> canonicalUris) {
        return Lists.newArrayList(Iterables.transform(canonicalUris, new Function<String, StoredEquivalenceResults>() {

            @Override
            public StoredEquivalenceResults apply(String input) {
                return forId(input);
            }

        }));
    }

    private String filenameFor(String canonicalUri) {
        return canonicalUri.replace('/', '-');
    }
}
