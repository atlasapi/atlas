package org.atlasapi.remotesite.pa.features;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.Unmarshaller.Listener;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.feeds.upload.FileUploadResult;
import org.atlasapi.feeds.upload.FileUploadResult.FileUploadResultType;
import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.features.PaFeaturesContentGroupProcessor.FeatureSetContentGroups;
import org.atlasapi.remotesite.pa.features.bindings.Feature;
import org.atlasapi.remotesite.pa.features.bindings.FeatureSet;
import org.atlasapi.remotesite.pa.features.bindings.Features;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.XMLReader;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

public class PaFeaturesUpdater extends ScheduledTask {
    
    private static final Duration UPCOMING_INTERVAL_DURATION = Duration.standardDays(2);
    private static final String SERVICE = "PA";
    private static final Pattern FILEDATE = Pattern.compile("^.*(\\d{8})_features.xml$");
    
    private final Logger log = LoggerFactory.getLogger(PaFeaturesUpdater.class);
    private final PaProgrammeDataStore dataStore;
    private final FileUploadResultStore fileUploadResultStore;
    private final PaFeaturesProcessor processor;
    private final XMLReader reader;
    private final PaFeaturesContentGroupProcessor contentGroupProcessor;
    
    public PaFeaturesUpdater(PaProgrammeDataStore dataStore, FileUploadResultStore fileUploadResultStore, 
            PaFeaturesProcessor processor, PaFeaturesContentGroupProcessor contentGroupProcessor) {
        this.dataStore = checkNotNull(dataStore);
        this.fileUploadResultStore = checkNotNull(fileUploadResultStore);
        this.processor = checkNotNull(processor);
        this.contentGroupProcessor = checkNotNull(contentGroupProcessor);
        this.reader = createReader();
    }

    private XMLReader createReader() {
        try {
            JAXBContext context = JAXBContext.newInstance("org.atlasapi.remotesite.pa.features.bindings");
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
        DateTime sixAmToday = LocalDate.now().toDateTime(LocalTime.MIDNIGHT).plusHours(6);
        contentGroupProcessor.prepareUpdate();
        processor.prepareUpdate(new Interval(sixAmToday, sixAmToday.plus(UPCOMING_INTERVAL_DURATION)));
        processFiles(dataStore.localFeaturesFiles(Predicates.<File>alwaysTrue()));
        contentGroupProcessor.finishUpdate();
    }
    
    private void processFiles(List<File> files) {
        UpdateProgress progress = UpdateProgress.START;
        try { 
            Iterator<File> fileIter = files.iterator();
            while (fileIter.hasNext() && shouldContinue()) {
                File file = fileIter.next();
                FileUploadResult result = processFile(file);
                reportStatus(progress.toString(String.format("Processed %%s of %s files (%%s failures), processing %s", files.size(), file.getName())));
                fileUploadResultStore.store(result.filename(), result);
                progress = progress.reduce(toProgress(result));
            }
        } catch (Exception e) {
            log.error("Exception running PA updater", e);
            // this will stop the task
            Throwables.propagate(e);
        }
        reportStatus(progress.toString(String.format("Processed %%s of %s files (%%s failures)", files.size())));
    }

    private UpdateProgress toProgress(FileUploadResult result) {
        return FileUploadResultType.SUCCESS.equals(result.type()) ? SUCCESS
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
            }  else {
                log.warn("Not processing file " + file.toString() + " as filename format is not recognised");
                result = FileUploadResult.failedUpload(SERVICE, file.getName()).withMessage("Format not recognised");
            }
        } catch (Exception e) {
            result = FileUploadResult.failedUpload(SERVICE, file.getName()).withCause(e);
            log.error("Error processing file " + file.toString(), e);
        }
        return result;
    }

    private Listener featuresProcessingListener() {
        return new Unmarshaller.Listener() {
            public void beforeUnmarshal(Object target, Object parent) { }

            public void afterUnmarshal(Object target, Object parent) {
                if (target instanceof FeatureSet) {
                    try {
                        FeatureSet featureSet = (FeatureSet) target;
                        Optional<FeatureSetContentGroups> contentGroups = contentGroupProcessor.getContentGroups(featureSet.getId());
                        if (contentGroups.isPresent()) {
                            log.info("Processing FeatureSet Id {}", featureSet.getId());
                            Features features = Iterables.getOnlyElement(featureSet.getFeatures());
                            for (Feature feature : features.getFeature()) {
                                processor.process(feature.getProgrammeID(), contentGroups.get());
                            }
                        } else {
                            log.error("FeatureSet Id {} not supported", featureSet.getId());
                        }
                    } catch (NoSuchElementException e) {
                        log.error("No content found for programme Id: " + ((Feature) target).getProgrammeID(), e);
                    }
                }
            }
        };
    }
}
