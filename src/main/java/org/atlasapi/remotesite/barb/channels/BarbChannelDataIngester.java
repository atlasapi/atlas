package org.atlasapi.remotesite.barb.channels;

import com.google.common.base.Throwables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

public class BarbChannelDataIngester extends ScheduledTask {

    private static final String MAS_FILENAME_FORMAT = "B%s.MAS.Z";

    private static final Logger log = LoggerFactory.getLogger(BarbChannelDataIngester.class);

    private final FTPClient ftpClient;

    private BarbChannelDataIngester(FTPClient ftpClient) {
        this.ftpClient = ftpClient;
    }

    public static BarbChannelDataIngester create(FTPClient ftpClient) {
        return new BarbChannelDataIngester(ftpClient);
    }

    @Override
    public void runTask() {

        try {
            File masFile = File.createTempFile("temp", "txt");

            checkArgument(masFile.exists());
            checkArgument(ftpClient.retrieveFile("/ClientFiles/", new FileOutputStream(masFile)));

        } catch (IOException e) {
            log.error("Error ingesting mas file", e);
            throw Throwables.propagate(e);
        }
    }
}
