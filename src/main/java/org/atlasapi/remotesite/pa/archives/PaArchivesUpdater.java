package org.atlasapi.remotesite.pa.archives;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.feeds.upload.FileUploadResult;
import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.remotesite.pa.archives.bindings.ArchiveUpdate;
import org.atlasapi.remotesite.pa.archives.bindings.ProgData;
import org.atlasapi.remotesite.pa.archives.bindings.TvData;
import org.atlasapi.remotesite.pa.channels.PaChannelDataHandler;
import org.atlasapi.remotesite.pa.channels.bindings.TvChannelData;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.features.PaFeaturesContentGroupProcessor;
import org.atlasapi.remotesite.pa.features.PaFeaturesProcessor;
import org.atlasapi.remotesite.pa.features.bindings.Feature;
import org.atlasapi.remotesite.pa.features.bindings.FeatureSet;
import org.atlasapi.remotesite.pa.features.bindings.Features;

import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.XMLReader;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

public class PaArchivesUpdater extends ScheduledTask {

    private static final Duration UPCOMING_INTERVAL_DURATION = Duration.standardDays(2);
    private static final String SERVICE = "PA";
    private static final Pattern FILEDATE = Pattern.compile("^.*(\\d{12})_tvarchive.xml$");

    private final Logger log = LoggerFactory.getLogger(PaArchivesUpdater.class);
    private final PaProgrammeDataStore dataStore;
    private final FileUploadResultStore fileUploadResultStore;
    private final PaArchivesProcessor processor;
    private final XMLReader reader;

    public PaArchivesUpdater(PaProgrammeDataStore dataStore,
            FileUploadResultStore fileUploadResultStore,
            PaArchivesProcessor processor) {
        this.dataStore = checkNotNull(dataStore);
        this.fileUploadResultStore = checkNotNull(fileUploadResultStore);
        this.processor = checkNotNull(processor);
        this.reader = createReader();
    }

    private XMLReader createReader() {
        try {
            JAXBContext context = JAXBContext.newInstance(
                    "org.atlasapi.remotesite.pa.archives.bindings");
            Unmarshaller unmarshaller = context.createUnmarshaller();
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            XMLReader reader = factory.newSAXParser().getXMLReader();
            reader.setContentHandler(unmarshaller.getUnmarshallerHandler());
            unmarshaller.setListener(featuresProcessingListener());
            return reader;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void runTask() {
        processFiles(dataStore.localArchivesFiles(Predicates.<File>alwaysTrue()));
    }

    private void processFiles(List<File> files) {
        UpdateProgress progress = UpdateProgress.START;
        try {
            Iterator<File> fileIter = files.iterator();
            while (fileIter.hasNext() && shouldContinue()) {
                File file = fileIter.next();
                FileUploadResult result = processFile(file);
                reportStatus(progress.toString(String.format(
                        "Processed %%s of %s files (%%s failures), processing %s",
                        files.size(),
                        file.getName()
                )));
                fileUploadResultStore.store(result.filename(), result);
                progress = progress.reduce(toProgress(result));
            }
        } catch (Exception e) {
            log.error("Exception running PA updater", e);
            // this will stop the task
            Throwables.propagate(e);
        }
        reportStatus(progress.toString(String.format(
                "Processed %%s of %s files (%%s failures)",
                files.size()
        )));
    }

    private UpdateProgress toProgress(FileUploadResult result) {
        return FileUploadResult.FileUploadResultType.SUCCESS.equals(result.type()) ? SUCCESS
                                                                                   : FAILURE;
    }

    private FileUploadResult processFile(File file) {
        FileUploadResult result;
        try {
            String filename = file.toURI().toString();
            Matcher matcher = FILEDATE.matcher(filename);
            if (matcher.matches()) {
                log.info("Processing file " + file.toString());
                File fileToProcess = dataStore.copyForProcessing(file);
                reader.parse(fileToProcess.toURI().toString());
                result = FileUploadResult.successfulUpload(SERVICE, file.getName());
            } else {
                log.warn("Not processing file "
                        + file.toString()
                        + " as filename format is not recognised");
                result = FileUploadResult.failedUpload(SERVICE, file.getName())
                        .withMessage("Format not recognised");
            }
        } catch (Exception e) {
            result = FileUploadResult.failedUpload(SERVICE, file.getName()).withCause(e);
            log.error("Error processing file " + file.toString(), e);
        }
        return result;
    }

    private Unmarshaller.Listener featuresProcessingListener() {
        return new Unmarshaller.Listener() {

            public void beforeUnmarshal(Object target, Object parent) {
            }

            public void afterUnmarshal(Object target, Object parent) {
                if (target instanceof TvData) {
                    try {
                        TvData archiveData = (TvData) target;
                        List<ArchiveUpdate> archiveUpdates = archiveData.getArchiveUpdate();
                        for (ArchiveUpdate archiveUpdate : archiveUpdates) {
                            log.info("Started processing PA updates for: "
                                    + archiveUpdate.getDate());
                            processor.process(archiveUpdate.getProgData(), archiveUpdate.getDate());
                        }
                    } catch (NoSuchElementException e) {
                        log.error("No content found for programme Id: "
                                + ((Feature) target).getProgrammeID(), e);
                    }
                }
            }
        };
    }
}