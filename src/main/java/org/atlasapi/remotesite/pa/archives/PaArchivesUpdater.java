package org.atlasapi.remotesite.pa.archives;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.feeds.upload.FileUploadResult;
import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.remotesite.pa.archives.bindings.ArchiveUpdate;
import org.atlasapi.remotesite.pa.archives.bindings.TvData;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.XMLReader;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

public class PaArchivesUpdater extends ScheduledTask {

    private static final String SERVICE = "PA";

    private static final DateTimeFormatter FILEDATETIME_FORMAT = DateTimeFormat.forPattern("yyyyMMdd-HH:mm").withZone(
            DateTimeZones.LONDON);
    private static final Pattern FILEDATETIME = Pattern.compile("^.*(\\d{12})_.+_tvarchive.xml$");

    private final Logger log = LoggerFactory.getLogger(PaArchivesUpdater.class);
    private final PaProgrammeDataStore dataStore;
    private final FileUploadResultStore fileUploadResultStore;
    private final PaUpdatesProcessor processor;
    private final PaDataToUpdatesTransformer transformer = new PaDataToUpdatesTransformer();

    public PaArchivesUpdater(PaProgrammeDataStore dataStore,
            FileUploadResultStore fileUploadResultStore,
            PaUpdatesProcessor processor) {
        this.dataStore = checkNotNull(dataStore);
        this.fileUploadResultStore = checkNotNull(fileUploadResultStore);
        this.processor = checkNotNull(processor);
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
            Matcher matcher = FILEDATETIME.matcher(filename);

            JAXBContext context = JAXBContext.newInstance(
                    "org.atlasapi.remotesite.pa.listings.bindings");
            Unmarshaller unmarshaller = context.createUnmarshaller();
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            XMLReader reader = factory.newSAXParser().getXMLReader();
            reader.setContentHandler(unmarshaller.getUnmarshallerHandler());

            if (matcher.matches()) {
                log.info("Processing file " + file.toString());
                File fileToProcess = dataStore.copyForProcessing(file);
                final String scheduleDay = matcher.group(1);
                unmarshaller.setListener(archivesProcessingListener(fileToProcess, scheduleDay));
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

    private Unmarshaller.Listener archivesProcessingListener(final File fileToProcess, final String fileData) {
        return new Unmarshaller.Listener() {

            public void beforeUnmarshal(Object target, Object parent) {
            }

            public void afterUnmarshal(Object target, Object parent) {
                if (target instanceof TvData) {
                    try {
                        TvData tvData = (TvData) target;
                        ArchiveUpdate archive = Iterables.getOnlyElement(tvData.getArchiveUpdate());
                        for (org.atlasapi.remotesite.pa.archives.bindings.ProgData progData : archive.getProgData()) {
                            log.info("Started processing PA updates for: "
                                    + progData.getProgId());
                            ProgData listings = transformer.transformToListingProgdata(progData);
                            processor.process(listings, getTimeZone(fileData),Timestamp.of(fileToProcess.lastModified()));
                        }

                    } catch (NoSuchElementException e) {
                        log.error("Failed to process " + fileData , e);
                    }
                }
            }
        };
    }

    protected static DateTimeZone getTimeZone(String date) {
        String timezoneDateString = date + "-11:00";
        DateTime timezoneDateTime = FILEDATETIME_FORMAT.parseDateTime(timezoneDateString);
        DateTimeZone zone = timezoneDateTime.getZone();
        return DateTimeZone.forOffsetMillis(zone.getOffset(timezoneDateTime));
    }
}