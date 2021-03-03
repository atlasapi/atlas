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
        String filename = filenameFor(results.subject().getCanonicalUri()) + ".html.gz";
        try {
            File tempFile = File.createTempFile(filename, null);
            try (FileOutputStream fos = new FileOutputStream(tempFile);
                 GZIPOutputStream gos = new GZIPOutputStream(fos);
                 ObjectOutputStream os = new ObjectOutputStream(gos)) {
                os.writeObject(storedEquivalenceResults);
            }
            String filePath = directoryFor(filename) + "/" + filename;
            s3Processor.uploadFile(filePath, tempFile);
            tempFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return storedEquivalenceResults;
    }

    @Override
    public StoredEquivalenceResults forId(String canonicalUri) {
        String filename = filenameFor(canonicalUri) + ".html.gz";
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

    public StoredEquivalenceResults moveResultsToS3(StoredEquivalenceResults storedEquivalenceResults, String filename) {
        filename = filename + ".html.gz";
        try {
            File tempFile = File.createTempFile(filename, null);
            try (FileOutputStream fos = new FileOutputStream(tempFile);
                 GZIPOutputStream gos = new GZIPOutputStream(fos);
                 ObjectOutputStream os = new ObjectOutputStream(gos)) {
                os.writeObject(storedEquivalenceResults);
            }
            String filePath = directoryFor(filename) + "/" + filename;
            s3Processor.uploadFile(filePath, tempFile);
            tempFile.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return storedEquivalenceResults;
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

        File firstPath = new File(String.format("%03d", firstDir));
        File secondPath = new File(firstPath, String.format("%03d", secondDir));

        if (!secondPath.isDirectory()) {
            secondPath.mkdirs();
        }

        return secondPath;
    }

}
