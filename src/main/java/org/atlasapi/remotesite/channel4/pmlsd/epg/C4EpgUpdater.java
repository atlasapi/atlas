package org.atlasapi.remotesite.channel4.pmlsd.epg;

import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi;
import org.atlasapi.reporting.OwlReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.Iterables;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.DayRange;
import com.metabroadcast.common.time.DayRangeGenerator;

public class C4EpgUpdater extends ScheduledTask {

    private final C4AtomApi c4AtomApi;
    private final DayRangeGenerator rangeGenerator;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final OwlReporter owlReporter;

    private C4EpgChannelDayUpdater channelDayUpdater;

    public C4EpgUpdater(C4AtomApi atomApi, C4EpgChannelDayUpdater updater, DayRangeGenerator generator, OwlReporter owlReporter) {
        this.c4AtomApi = atomApi;
        this.channelDayUpdater = updater;
        this.rangeGenerator = generator;
        this.owlReporter = owlReporter;
    }

    @Override
    protected void runTask() {
        DateTime start = new DateTime(DateTimeZones.UTC);
        log.info("C4 EPG Update initiated");

        owlReporter.getTelescopeReporter().startReporting();
        log.info("C4 EPG Update started telescope reporting");
        
        DayRange dayRange = rangeGenerator.generate(new LocalDate(DateTimeZones.UTC));
        
        BiMap<String, Channel> channelMap = c4AtomApi.getChannelMap();
		int total = Iterables.size(dayRange) * channelMap.size();
        int processed = 0;
        UpdateProgress progress = UpdateProgress.START;
        
        for (Map.Entry<String, Channel> channelEntry : channelMap.entrySet()) {
            for (LocalDate scheduleDay : dayRange) {
                reportStatus(progressReport("Processing", processed++, total, progress));
                progress = progress.reduce(channelDayUpdater.update(channelEntry.getKey(), channelEntry.getValue(), scheduleDay));
            }
        }
        
        reportStatus(progressReport("Processed", processed++, total, progress));
        String runTime = new Period(start, new DateTime(DateTimeZones.UTC)).toString(PeriodFormat.getDefault());
        log.info("C4 EPG Update finished in " + runTime);

        owlReporter.getTelescopeReporter().endReporting();
        log.info("C4 EPG Update ended telescope reporting");

        if (progress.hasFailures()) {
            throw new IllegalStateException(String.format("Completed with %s failures", progress.getFailures()));
        }
    }

    private String progressReport(String prefix, int processed, int total, UpdateProgress progress) {
        return String.format("%s %s/%s. %s failures. %s broadcasts processed", prefix, processed, total, progress.getFailures(), progress.getProcessed());
    }

}
