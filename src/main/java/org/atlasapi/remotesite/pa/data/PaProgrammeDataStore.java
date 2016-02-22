package org.atlasapi.remotesite.pa.data;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.net.ftp.FTPFile;

import com.google.common.base.Predicate;

public interface PaProgrammeDataStore {

    boolean requiresUpdating(FTPFile file);
    
    void save(String fileName, InputStream dataStream) throws Exception;
    
    List<File> localTvDataFiles(Predicate<File> filter);
    
    List<File> localFeaturesFiles(Predicate<File> filter);
    
    List<File> localProfilesFiles(Predicate<File> filter);

    List<File> localChannelsFiles(Predicate<File> filter);

    List<File> localArchivesFiles(Predicate<File> filter);
    
    File copyForProcessing(File file);
}
