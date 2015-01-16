package org.atlasapi.remotesite.pa.channels;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.Unmarshaller.Listener;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.feeds.upload.FileUploadResult;
import org.atlasapi.remotesite.pa.channels.bindings.TvChannelData;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.XMLReader;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.time.DateTimeZones;

public class PaChannelsUpdater extends ScheduledTask {
    
    private static final String SERVICE = "PA";
    private static final Pattern FILEDATE = Pattern.compile("^.*(\\d{12})_tv_channel_data.xml$");
    private static final DateTimeFormatter FILEDATETIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm").withZone(DateTimeZones.LONDON);

    private final PaProgrammeDataStore dataStore;
    private final PaChannelDataHandler channelDataHandler;
    private final Logger log = LoggerFactory.getLogger(PaChannelsUpdater.class);
    private final Lock channelWriterLock;

    public PaChannelsUpdater(PaProgrammeDataStore dataStore, PaChannelDataHandler channelDataHandler, Lock channelWriterLock) {
        this.dataStore = checkNotNull(dataStore);
        this.channelDataHandler = checkNotNull(channelDataHandler);
        this.channelWriterLock = checkNotNull(channelWriterLock);
    }
    
    @Override
    public void runTask() {
        try {
            log.info("Acquiring channel writer lock");
            channelWriterLock.lock();
            log.info("Acquired channel writer lock");
            processFiles(dataStore.localChannelsFiles(Predicates.<File>alwaysTrue()));
        } finally {
            channelWriterLock.unlock();
        }
    }
    
    public void processFiles(Iterable<File> files) {
        try { 
            JAXBContext context = JAXBContext.newInstance("org.atlasapi.remotesite.pa.channels.bindings");
            Unmarshaller unmarshaller = context.createUnmarshaller();

            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            XMLReader reader = factory.newSAXParser().getXMLReader();
            reader.setContentHandler(unmarshaller.getUnmarshallerHandler());

            File latestFile = null;
            DateTime latestTime = null;

            for (File file : files) {
                if(!shouldContinue()) {
                    break;
                }

                final String filename = file.toURI().toString();
                Matcher matcher = FILEDATE.matcher(filename);

                if (matcher.matches()) {
                    DateTime startTime = FILEDATETIME_FORMAT.parseDateTime(matcher.group(1));
                    if (latestTime == null) {
                        latestFile = file;
                        latestTime = startTime;
                    } else {
                        if (startTime.isAfter(latestTime)) {
                            // log a failed result for the previous latest file
                            log.info("Not processing file " + latestFile.toString() + " as file has been superseded by one that is more recent");
                            FileUploadResult.failedUpload(SERVICE, latestFile.getName()).withMessage("Outdated file");
                            latestFile = file;
                            latestTime = startTime;
                        }
                    }
                } else {
                    log.info("Not processing file " + file.toString() + " as filename format is not recognised");
                    FileUploadResult.successfulUpload(SERVICE, file.getName()).withMessage("Format not recognised");
                }
            }

            if (latestFile == null) {
                throw new RuntimeException("Found no files which matched"); 
            }

            try {        
                log.info("Processing file " + latestFile.toString());
                reportStatus(String.format("processed latest file: %s", latestFile.getName()));
                final File fileToProcess = dataStore.copyForProcessing(latestFile);
                unmarshaller.setListener(channelsProcessingListener());
                reader.parse(fileToProcess.toURI().toString());

                FileUploadResult.successfulUpload(SERVICE, latestFile.getName());
            } catch (Exception e) {
                FileUploadResult.failedUpload(SERVICE, latestFile.getName()).withCause(e);
                Throwables.propagate(e);
            }

            reportStatus(String.format("found %s files, processed latest file: %s", Iterables.size(files), latestFile.getName()));
        } catch (Exception e) {
            log.error("Exception running PA channels updater", e);
            Throwables.propagate(e);
        }
    }

    private Listener channelsProcessingListener() {
        return new Unmarshaller.Listener() {
            public void beforeUnmarshal(Object target, Object parent) {
            }

            public void afterUnmarshal(Object target, Object parent) {
                if (target instanceof TvChannelData) {
                    channelDataHandler.handle((TvChannelData) target);
                }
            }
        };
    }

}
