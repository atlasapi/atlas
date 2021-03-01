package org.atlasapi.equiv.results.persistence;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;

import java.io.*;
import java.util.List;

/**
 * Filesystem-based store for equivalence results. New files will be stored in a
 * hashed directory structure to avoid a single directory with too many files in it, but
 * an attempt will also be made to read directly from the root directory for old results.
 */
public class CombinedEquivalenceResultStore implements EquivalenceResultStore {

    private StoredEquivalenceResultsTranslator translator = new StoredEquivalenceResultsTranslator();
    private final File baseDirectory;
    private S3EquivalenceResultStore s3EquivalenceResultStore;

    public CombinedEquivalenceResultStore(File directory) {
        if(!directory.isDirectory()) {
            throw new IllegalArgumentException("Must be a directory");
        }
        if(!directory.exists()) {
            throw new IllegalArgumentException("Directory does not exist");
        }
        this.baseDirectory = directory;
        this.s3EquivalenceResultStore = new S3EquivalenceResultStore(directory);
    }
    
    @Override
    public <T extends Content> StoredEquivalenceResults store(
            EquivalenceResults<T> results) {
        StoredEquivalenceResults storedEquivalenceResults = translator.toStoredEquivalenceResults(results);
        try {
            String filename = filenameFor(results.subject().getCanonicalUri());
            ObjectOutputStream os = new ObjectOutputStream(
                    new FileOutputStream(new File(directoryFor(filename), filename)));
            os.writeObject(storedEquivalenceResults);
            os.close();
            s3EquivalenceResultStore.store(results);
        } catch (FileNotFoundException e) {
            Throwables.propagate(e);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        
        return storedEquivalenceResults;
    }

    @Override
    public StoredEquivalenceResults forId(String canonicalUri) {
        String filename = filenameFor(canonicalUri);

        Optional<StoredEquivalenceResults> resultInHashedDirectory = resultFor(new File(directoryFor(filename), filename));
        if (resultInHashedDirectory.isPresent()) {
            return resultInHashedDirectory.get();
        }

        Optional<StoredEquivalenceResults> resultInBaseDirectory = resultFor(new File(baseDirectory, filename));
        return resultInBaseDirectory.orNull();
    }

    private Optional<StoredEquivalenceResults> resultFor(File file) {
        if(!file.exists()) {
            return Optional.absent();
        }

        try(ObjectInputStream is = new ObjectInputStream(new FileInputStream(file))) {
            Object readObject = is.readObject();
            try {
                return Optional.of((StoredEquivalenceResults) readObject);
            } catch (ClassCastException e) { //Stored object might be the old result object
                return Optional.of(new StoredEquivalenceResults((StoredEquivalenceResult) readObject));
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        } catch (ClassNotFoundException e) {
            Throwables.propagate(e);
        }
        return Optional.absent();
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

        File firstPath = new File(baseDirectory, String.format("%03d", firstDir));
        File secondPath = new File(firstPath, String.format("%03d", secondDir));

        if (!secondPath.isDirectory()) {
            secondPath.mkdirs();
        }

        return secondPath;
    }

}
