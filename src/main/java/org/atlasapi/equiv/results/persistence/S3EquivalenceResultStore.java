package org.atlasapi.equiv.results.persistence;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.sun.istack.Nullable;
import org.atlasapi.equiv.S3Processor;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;

import java.io.*;
import java.util.List;
import java.util.Optional;

/**
 * Filesystem-based store for equivalence results. New files will be stored in a
 * hashed directory structure to avoid a single directory with too many files in it, but
 * an attempt will also be made to read directly from the root directory for old results.
 */
public class S3EquivalenceResultStore implements EquivalenceResultStore {

    private StoredEquivalenceResultsTranslator translator = new StoredEquivalenceResultsTranslator();
    private final File baseDirectory;
    private final S3Processor s3Processor;

    public S3EquivalenceResultStore(File directory) {
        if(!directory.isDirectory()) {
            throw new IllegalArgumentException("Must be a directory");
        }
        if(!directory.exists()) {
            throw new IllegalArgumentException("Directory does not exist");
        }
        this.baseDirectory = directory;
        this.s3Processor = S3Processor.create();
    }
    
    @Override
    public <T extends Content> StoredEquivalenceResults store(
            EquivalenceResults<T> results) {
        StoredEquivalenceResults storedEquivalenceResults = translator.toStoredEquivalenceResults(results);
        String filename = filenameFor(results.subject().getCanonicalUri());
        String filePath = directoryFor(filename) + "/" + filename;
        s3Processor.uploadFile(filePath, new File(filename));

        return storedEquivalenceResults;
    }

    @Override
    public StoredEquivalenceResults forId(String canonicalUri) {
        String filename = filenameFor(canonicalUri);
        String filePath = directoryFor(filename) + "/" + filename;

        Optional<StoredEquivalenceResults> resultInHashedDirectory = s3Processor.getStoredEquivalenceResults(filePath);
        if (resultInHashedDirectory.isPresent()) {
            return resultInHashedDirectory.get();
        }

        filePath = baseDirectory + "/" + filename;
        Optional<StoredEquivalenceResults> resultInBaseDirectory = s3Processor.getStoredEquivalenceResults(filePath);
        return resultInBaseDirectory.orElse(null);
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

    /**
     * Create a hashed directory for the given filename, creating if necessary
     */
    private File directoryFor(String filename) {

        // Inspired by http://michaelandrews.typepad.com/the_technical_times/2009/10/creating-a-hashed-directory-structure.html
        int hashcode = filename.hashCode();
        int mask = 255;
        int firstDir = hashcode & mask;
        int secondDir = (hashcode >> 8) & mask;

        File firstPath = new File(s3Processor.getS3BucketUpload(), String.format("%03d", firstDir));
        File secondPath = new File(firstPath, String.format("%03d", secondDir));

        if (!secondPath.isDirectory()) {
            secondPath.mkdirs();
        }

        return secondPath;
    }

}
