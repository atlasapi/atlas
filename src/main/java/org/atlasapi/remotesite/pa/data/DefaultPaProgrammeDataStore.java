package org.atlasapi.remotesite.pa.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPFile;
import org.atlasapi.s3.S3Client;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.base.Maybe;

public class DefaultPaProgrammeDataStore implements PaProgrammeDataStore {

    private static final String TV_LISTINGS_DTD = "TVListings.dtd";
    private static final String FEATURES_DTD = "features.dtd";
    private static final String TV_CHANNEL_DATA_DTD = "tv_channel_data.dtd";
    private static final String TV_ARCHIVES_DTD = "TVArchive.dtd";
    private static final FilenameFilter tvDataFilenameFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("_tvdata.xml");
        }
    };
    private static final FilenameFilter featuresFilenameFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("_features.xml");
        }
    };

    private static final FilenameFilter profilesFilenameFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("_profiles.xml") || name.endsWith("_profiles_full_dump.xml");
        }
    };

    private static final FilenameFilter channelsFilenameFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("_tv_channel_data.xml");
        }
    };

    private static final FilenameFilter archivesFilenameFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("_tvarchive.xml");
        }
    };

    private static final FilenameFilter activeIdArchive = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("_archiveID.xml");
        }
    };

    private static final String PROCESSING_DIR = "/processing";

    private final File localFolder;
    private final File processingFolder;
    private final S3Client s3client;

    public DefaultPaProgrammeDataStore(String localFilesPath, S3Client s3client) {
        checkNotNull(localFilesPath);
        this.s3client = s3client;
        this.localFolder = new File(localFilesPath);
        if (!localFolder.exists()) {
            localFolder.mkdir();
        }
        if (!localFolder.isDirectory()) {
            throw new IllegalArgumentException("Files path is not a directory: " + localFilesPath);
        }
        
        this.processingFolder = new File(localFilesPath + PROCESSING_DIR);
        if (!processingFolder.exists()) {
            processingFolder.mkdir();
        }
        if (!processingFolder.isDirectory()) {
            throw new IllegalArgumentException("Processing folder is not a directory: " + localFilesPath + PROCESSING_DIR);
        }
        
        loadDtdFile(TV_LISTINGS_DTD);
        loadDtdFile(FEATURES_DTD);
        loadDtdFile(TV_CHANNEL_DATA_DTD);
        loadDtdFile(TV_ARCHIVES_DTD);
    }
    
    private void loadDtdFile(String dtdFileName) {
        File dtdFile = new File(localFolder, dtdFileName);
        if (! dtdFile.exists()) {
            InputStream resourceAsStream = null;
            try {
                resourceAsStream = DefaultPaProgrammeDataStore.class.getClassLoader().getResourceAsStream(dtdFileName);
            } catch (NullPointerException e) {
                resourceAsStream = DefaultPaProgrammeDataStore.class.getClassLoader().getParent().getResourceAsStream(dtdFileName);
            }
            
            if (resourceAsStream != null) {
                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(dtdFile);
                    IOUtils.copy(resourceAsStream, fos);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (fos != null) {
                        try {
                            fos.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        
        if (dtdFile.exists()) {
            try {
                FileUtils.copyFileToDirectory(dtdFile, processingFolder);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean requiresUpdating(FTPFile ftpFile) {
        String name = ftpFile.getName();
        Maybe<File> existingFile = findExistingFile(name);
        return existingFile.isNothing() || moreRecent(ftpFile, existingFile.requireValue()) || differentSizes(ftpFile, existingFile.requireValue());
    }

    @Override
    public void save(String fileName, InputStream dataStream) throws Exception {
        FileOutputStream fos = null;
        try {
            File localFile = new File(localFolder, fileName);
            fos = new FileOutputStream(localFile);
            IOUtils.copy(dataStream, fos);
            s3client.put(fileName, localFile);
        } finally {
            if (fos != null) {
                fos.close();
            }
        }
    }

    private boolean differentSizes(FTPFile ftpFile, File existingFile) {
        return ftpFile.getSize() != existingFile.length();
    }

    private boolean moreRecent(FTPFile ftpFile, File existingFile) {
        return ftpFile.getTimestamp().getTime().after(new Date(existingFile.lastModified()));
    }

    @Override
    public List<File> localTvDataFiles(Predicate<File> filter) {
        Predicate<File> fileFilter = filter == null ? Predicates.<File> alwaysTrue() : filter;
        ImmutableList<File> tvDataFiles = ImmutableList.copyOf(
                localFolder.listFiles(tvDataFilenameFilter)
        );
        return FILE_NAME_ORDER.immutableSortedCopy(Iterables.filter(tvDataFiles, fileFilter));
    }

    @Override
    public List<File> localFeaturesFiles(Predicate<File> filter) {
        Predicate<File> fileFilter = filter == null ? Predicates.<File> alwaysTrue() : filter;
        ImmutableList<File> featuresFiles = ImmutableList.copyOf(
                localFolder.listFiles(featuresFilenameFilter)
        );
        return FILE_NAME_ORDER.immutableSortedCopy(Iterables.filter(featuresFiles, fileFilter));
    }

    @Override
    public List<File> localProfilesFiles(Predicate<File> filter) {
        Predicate<File> fileFilter = filter == null ? Predicates.<File> alwaysTrue() : filter;
        ImmutableList<File> profileFiles = ImmutableList.copyOf(
                localFolder.listFiles(profilesFilenameFilter)
        );
        return FILE_NAME_ORDER.immutableSortedCopy(Iterables.filter(profileFiles, fileFilter));
    }
    
    @Override
    public List<File> localChannelsFiles(Predicate<File> filter) {
        Predicate<File> fileFilter = filter == null ? Predicates.<File> alwaysTrue() : filter;
        ImmutableList<File> channelsFiles = ImmutableList.copyOf(
                localFolder.listFiles(channelsFilenameFilter)
        );
        return FILE_NAME_ORDER.immutableSortedCopy(Iterables.filter(channelsFiles, fileFilter));
    }

    @Override
    public List<File> localArchivesFiles(Predicate<File> filter) {
        Predicate<File> fileFilter = filter == null ? Predicates.<File> alwaysTrue() : filter;
        ImmutableList<File> archiveFiles = ImmutableList.copyOf(
                localFolder.listFiles(archivesFilenameFilter)
        );
        return FILE_NAME_ORDER.immutableSortedCopy(Iterables.filter(archiveFiles, fileFilter));
    }

    @Override
    public List<File> localActiveIdArchiveFiles(Predicate<File> filter) {
        Predicate<File> fileFilter = filter == null ? Predicates.<File> alwaysTrue() : filter;
        ImmutableList<File> activeIdArchives = ImmutableList.copyOf(
                localFolder.listFiles(activeIdArchive)
        );
        return FILE_NAME_ORDER.immutableSortedCopy(Iterables.filter(activeIdArchives, fileFilter));
    }

    private final Ordering<File> FILE_NAME_ORDER = new Ordering<File>() {
        @Override
        public int compare(File left, File right) {
            return left.getName().compareTo(right.getName());
        }
    };

    private Maybe<File> findExistingFile(String fileName) {
        for (File file : localFolder.listFiles()) {
            if (fileName.equals(file.getName())) {
                return Maybe.just(file);
            }
        }

        return Maybe.nothing();
    }

    @Override
    public File copyForProcessing(File file) {
        Preconditions.checkArgument(file.isFile());
        Preconditions.checkArgument(!file.getPath().startsWith(this.processingFolder.getPath()));

        File forProcessing = new File(processingFolder, file.getName());
        try {
            FileUtils.copyFile(file, forProcessing);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Preconditions.checkArgument(forProcessing.exists());
        Preconditions.checkArgument(forProcessing.isFile());

        return forProcessing;
    }
}
