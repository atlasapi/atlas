package org.atlasapi.equiv.results.persistence;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Filesystem-based store for equivalence results. New files will be stored in a
 * hashed directory structure to avoid a single directory with too many files in it, but
 * an attempt will also be made to read directly from the root directory for old results.
 */
public class FileEquivalenceResultStore implements EquivalenceResultStore {

    private StoredEquivalenceResultTranslator translator = new StoredEquivalenceResultTranslator();
    private final File baseDirectory;
    
    public FileEquivalenceResultStore(File directory) {
        if(!directory.isDirectory()) {
            throw new IllegalArgumentException("Must be a directory");
        }
        if(!directory.exists()) {
            throw new IllegalArgumentException("Directory does not exist");
        }
        this.baseDirectory = directory;
    }
    
    @Override
    public <T extends Content> StoredEquivalenceResult store(
            EquivalenceResult<T> result) {
        StoredEquivalenceResult storedEquivalenceResult = translator.toStoredEquivalenceResult(result);
        try {
            String filename = filenameFor(result.subject().getCanonicalUri());
            ObjectOutputStream os = new ObjectOutputStream(
                    new FileOutputStream(new File(directoryFor(filename), filename)));
            os.writeObject(storedEquivalenceResult);
            os.close();
        } catch (FileNotFoundException e) {
            Throwables.propagate(e);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        
        return storedEquivalenceResult;
    }

    @Override
    public StoredEquivalenceResult forId(String canonicalUri) {
        String filename = filenameFor(canonicalUri);

        Optional<StoredEquivalenceResult> resultInHashedDirectory = resultFor(new File(directoryFor(filename), filename));
        if (resultInHashedDirectory.isPresent()) {
            return resultInHashedDirectory.get();
        }

        Optional<StoredEquivalenceResult> resultInBaseDirectory = resultFor(new File(baseDirectory, filename));
        return resultInBaseDirectory.orNull();
    }

    private Optional<StoredEquivalenceResult> resultFor(File file) {
        if(!file.exists()) {
            return Optional.absent();
        }

        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(file));
            return Optional.of((StoredEquivalenceResult) is.readObject());
        } catch (IOException e) {
            Throwables.propagate(e);
        } catch (ClassNotFoundException e) {
            Throwables.propagate(e);
        }
        return Optional.absent();
    }

    @Override
    public List<StoredEquivalenceResult> forIds(Iterable<String> canonicalUris) {
        return Lists.newArrayList(Iterables.transform(canonicalUris, new Function<String, StoredEquivalenceResult>() {

            @Override
            public StoredEquivalenceResult apply(String input) {
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
