package org.atlasapi.remotesite.pa;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PreDestroy;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.Unmarshaller.Listener;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.atlasapi.feeds.upload.FileUploadResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.listings.bindings.ChannelData;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

public abstract class PaBaseProgrammeUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(PaBaseProgrammeUpdater.class);
    
    private static final DateTimeFormatter FILEDATETIME_FORMAT = DateTimeFormat.forPattern("yyyyMMdd-HH:mm").withZone(DateTimeZones.LONDON);
    private static final DateTimeFormatter FILEDATE_FORMAT = DateTimeFormat.forPattern("yyyyMMdd");
    private static final DateTimeFormatter CHANNELINTERVAL_FORMAT = ISODateTimeFormat.dateTimeParser().withZone(DateTimeZones.LONDON);
    protected static final String SERVICE = "PA";
    
    private static final Pattern FILEDATE = Pattern.compile("^.*(\\d{8})_tvdata.xml$");

    private final PaChannelMap channelMap;
    private final Set<String> currentlyProcessing = Sets.newHashSet();
    
    private List<Channel> supportedChannels = ImmutableList.of();

    private final ExecutorService executor;
    private final PaProgrammeDataStore dataStore;
    private final PaChannelProcessor processor;
    private final PaDeltaFileHelper deltaFileHelper;
    private final Optional<PaScheduleVersionStore> paScheduleVersionStore;

    public PaBaseProgrammeUpdater(ExecutorService executor, PaChannelProcessor processor, PaProgrammeDataStore dataStore, ChannelResolver channelResolver, Optional<PaScheduleVersionStore> paScheduleVersionStore) {
        this.executor = executor;
        this.processor = processor;
        this.dataStore = dataStore;
        this.paScheduleVersionStore = paScheduleVersionStore;
        this.deltaFileHelper = new PaDeltaFileHelper();
        this.channelMap = new PaChannelMap(channelResolver);
    }

    public void supportChannels(Iterable<Channel> channels) {
        supportedChannels = ImmutableList.copyOf(channels);
    }
    
    private boolean isSupported(Channel channel) {
        if (supportedChannels.isEmpty() || supportedChannels.contains(channel)) {
            return true;
        }
        return false;
    }
    
    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }
    
    protected void processFiles(Iterable<File> files) {
    	
        try {       	
        	Set<Queue<File>> groupedFiles = deltaFileHelper.groupAndOrderFilesByDay(files);

        	boolean finished = false;
        	int filesProcessed = 0;
        	while (shouldContinue() && !finished) {
        		Builder<File> batch = ImmutableSet.builder();
        		for(Queue<File> day : groupedFiles) {
        			if(!day.isEmpty()) {
        				batch.add(day.remove());
        			}
        		}
        		Set<File> thisBatch = batch.build();
        		
        		if(!thisBatch.isEmpty()) {
        			reportStatus(String.format("%s/%s files processed. %s files in current batch", filesProcessed, Iterables.size(files), thisBatch.size()));
        			processBatch(thisBatch);
        			filesProcessed += thisBatch.size();
        		}
        		else {
        			finished = true;
        		}
        	}
            
        	reportStatus(String.format("%s files processed.", filesProcessed));
        } catch (Exception e) {
            log.error("Exception running PA updater", e);
        }
    }

	private void processBatch(Iterable<File> files) throws JAXBException,
			SAXException, ParserConfigurationException, InterruptedException {
		final CompletionService<Integer> completion = new ExecutorCompletionService<Integer>(executor);
		
		JAXBContext context = JAXBContext.newInstance("org.atlasapi.remotesite.pa.listings.bindings");
		Unmarshaller unmarshaller = context.createUnmarshaller();

		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setNamespaceAware(true);
		XMLReader reader = factory.newSAXParser().getXMLReader();
		reader.setContentHandler(unmarshaller.getUnmarshallerHandler());
		
		List<Future<Integer>> submitted = Lists.newArrayList();

		Map<String, AtomicInteger> jobsRemainingPerFile = Maps.newHashMap();
		
		for (File file : files) {
		    if(!shouldContinue()) {
		        break;
		    }
		    try {
		        final String filename = file.toURI().toString();
		        Matcher matcher = FILEDATE.matcher(filename);
		        
		        if (matcher.matches()) {
		            log.info("Processing file " + file.toString());
		            final File fileToProcess = dataStore.copyForProcessing(file);
		            final String scheduleDay = matcher.group(1);

		            AtomicInteger jobsRemaining = new AtomicInteger();
		            jobsRemainingPerFile.put(file.getName(), jobsRemaining);
		            
		            unmarshaller.setListener(channelDataProcessingListener(jobsRemaining, completion, submitted, fileToProcess, scheduleDay));
		            reader.parse(fileToProcess.toURI().toString());
		        }
		        else {
		            log.info("Not processing file " + file.toString() + " as filename format is not recognised");
		            storeResult(FileUploadResult.failedUpload(SERVICE, file.getName()).withMessage("Format not recognised"));
		        }
		    } catch (Exception e) {
		        storeResult(FileUploadResult.failedUpload(SERVICE, file.getName()).withCause(e));
		        log.error("Error processing file " + file.toString(), e);
		    }
		}
		
		if (!shouldContinue()) {
		    cancelTasks(submitted);
		    return;
		}
		
		int programmesProcessed = 0;
		int submitCount = submitted.size();
		for (int i = 0; i < submitCount && shouldContinue();) {
		    Future<Integer> processed = completion.poll(5, TimeUnit.SECONDS);
		    if(processed != null) {
		        i++;
		        try {
		            if (!processed.isCancelled()) {
		                programmesProcessed += processed.get();
		            }
		        } catch (Exception e) {
		            log.error("Exception processing PA updater results", e);
		        }
		        reportStatus(String.format("%s files. Processed %s/%s jobs. %s programmes processed", Iterables.size(files), i, submitCount, programmesProcessed));
		    }
		}
		
		if (!shouldContinue()) {
		    cancelTasks(submitted);
		    return;
		} else {
		    for (Entry<String, AtomicInteger> entry : jobsRemainingPerFile.entrySet()) {
		        String fileName = entry.getKey();
		        FileUploadResult result;
		        if (entry.getValue().get() == 0) {
		            result = FileUploadResult.successfulUpload(SERVICE, fileName);
		        } else {
		            result = FileUploadResult.failedUpload(SERVICE, fileName);
		        }
		        storeResult(result);
		    }
		}
	}

    private void cancelTasks(List<Future<Integer>> submitted) {
        reportStatus("Cancelling jobs");
        for (Future<Integer> future : submitted) {
            future.cancel(false);
        }
        reportStatus("Jobs cancelled");
    }

    private Listener channelDataProcessingListener(final AtomicInteger jobsCounter, 
            final CompletionService<Integer> completion, final List<Future<Integer>> submitted, 
            final File fileToProcess, final String fileDate) {
        
        return new Unmarshaller.Listener() {
            public void beforeUnmarshal(Object target, Object parent) {
            }

            public void afterUnmarshal(Object target, Object parent) {
                if (target instanceof ChannelData) {
                    
                    ChannelData channelData = (ChannelData) target;
                    
                    Interval schedulePeriod;
                    if(channelData.getStartTime() == null) {
                        // Old format channel file, we revert to guessing the 
                        // schedule period
                        DateTime startTime = FILEDATETIME_FORMAT.parseDateTime(fileDate+"-06:00");
                        schedulePeriod = new Interval(startTime, startTime.plusDays(1));                       
                    }
                    else {
                        schedulePeriod = new Interval(CHANNELINTERVAL_FORMAT.parseDateTime(channelData.getStartTime()), CHANNELINTERVAL_FORMAT.parseDateTime(channelData.getEndTime()));
                    }
                   
                    Maybe<Channel> channel = channelMap.getChannel(Integer.valueOf(channelData.getChannelId()));
                    
                    LocalDate scheduleDay = LocalDate.parse(fileDate, FILEDATE_FORMAT);
                    long version = deltaFileHelper.versionNumber(fileToProcess);
                    if (channel.hasValue() 
                            && isSupported(channel.requireValue()) 
                            && shouldContinue()
                            && shouldUpdateVersion(channel.requireValue(), version, scheduleDay)) {
                        
                        try {
                            
                            final PaChannelData data = new PaChannelData(
                                    channel.requireValue(),
                                    channelData.getProgData(), 
                                    schedulePeriod, 
                                    getTimeZone(fileDate), 
                                    Timestamp.of(fileToProcess.lastModified()),
                                    scheduleDay,
                                    version
                            );
                            jobsCounter.incrementAndGet();
                            Future<Integer> future = completion.submit(new Callable<Integer>() {
                                @Override
                                public Integer call() {
                                    try {
                                        return processor.process(data, currentlyProcessing);
                                    } finally {
                                        jobsCounter.decrementAndGet();
                                    }
                                }
                            });
                            
                            submitted.add(future);
                        } catch (Throwable e) {
                            log.error("Exception submitting PA channel update job in file " + fileToProcess.getName(), e);
                        }
                    }
                }
            }
        };
    }

    protected boolean shouldUpdateVersion(Channel channel,
            long fileVersionNumber, LocalDate scheduleDay) {
        
        if(!paScheduleVersionStore.isPresent()) {
            return true;
        }

        Optional<Long> currentVersion = paScheduleVersionStore.get().get(channel, scheduleDay);
        
        return !currentVersion.isPresent() || currentVersion.get() < fileVersionNumber;
    }

    public static class PaChannelData {
        
        private final Channel channel;
        private final Iterable<ProgData> programmes;
        private final Interval schedulePeriod;
        private final DateTimeZone zone;
        private final Timestamp lastUpdated;
        private final LocalDate scheduleDay;
        private final long version;

        public PaChannelData(Channel channel, Iterable<ProgData> programmes, Interval schedulePeriod, DateTimeZone zone, Timestamp lastUpdated, LocalDate scheduleDay, long version) {
            this.channel = channel;
            this.programmes = programmes;
            this.schedulePeriod = schedulePeriod;
            this.zone = zone;
            this.lastUpdated = lastUpdated;
            this.scheduleDay = scheduleDay;
            this.version = version;
        }

        public Channel channel() {
            return channel;
        }

        public Iterable<ProgData> programmes() {
            return programmes;
        }

        public Interval schedulePeriod() {
            return schedulePeriod;
        }

        public DateTimeZone zone() {
            return zone;
        }

        public Timestamp lastUpdated() {
            return lastUpdated;
        }
        
        public LocalDate scheduleDay() {
            return scheduleDay;
        }
        
        public long version() {
            return version;
        }
        
    }
    
    protected void storeResult(FileUploadResult result) {
        // do nothing. subclasses may choose to implement
    }

    /**
     * The PA do not supply timezone information with times. Instead all times for a file are consistently either GMT or BST,
     * based on the timezone of the listings_date. For example, for listings_date of the Saturday of the switch from BST to 
     * GMT, all times will be GMT+1h, even those after 0200. So 0500 London time on the Sunday is really in GMT, but a 
     * programme starting at 0500 in London will be listed as starting it 0600 (GMT+1)/
     * 
     * @param date
     * @return
     */
    protected static DateTimeZone getTimeZone(String date) {
        String timezoneDateString = date + "-11:00";
        DateTime timezoneDateTime = FILEDATETIME_FORMAT.parseDateTime(timezoneDateString);
        DateTimeZone zone = timezoneDateTime.getZone();
        return DateTimeZone.forOffsetMillis(zone.getOffset(timezoneDateTime));
    }
}
