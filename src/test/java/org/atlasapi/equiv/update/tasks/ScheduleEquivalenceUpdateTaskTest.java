package org.atlasapi.equiv.update.tasks;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Test;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleEquivalenceUpdateTaskTest {

    @SuppressWarnings("unchecked") 
    private final EquivalenceUpdater<Content> updater = mock(EquivalenceUpdater.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    @Mock private OwlTelescopeReporter telescope = mock(OwlTelescopeReporter.class);

    // TODO make this take multiple schedules
    private final ScheduleResolver scheduleResolver(final Schedule schedule) {
        return new ScheduleResolver() {
            
            @Override
            public Schedule schedule(DateTime from, DateTime to, Iterable<Channel> channels,
                    Iterable<Publisher> publisher, Optional<Application> mergeApp) {
                return schedule;
            }
            
            @Override
            public Schedule schedule(DateTime from, int count, Iterable<Channel> channels,
                    Iterable<Publisher> publisher, Optional<Application> mergeApp) {
                return schedule;
            }
            
            @Override
            public Schedule unmergedSchedule(DateTime from, DateTime to,
                    Iterable<Channel> channels, Iterable<Publisher> publisher) {
                return schedule;
            }
        };
    };
    
    @Test
    public void testUpdateScheduleEquivalences() {
        
        Channel bbcOne = new Channel(Publisher.METABROADCAST, "BBC One", "bbcone", false, MediaType.VIDEO, "bbconeuri");
        
        Item yvItemOne = new Item("yv1", "yv1c", Publisher.YOUVIEW);
        Version version = new Version();
        Broadcast broadcast = new Broadcast("bbcone", DateTime.now(), DateTime.now());
        version.addBroadcast(broadcast);
        yvItemOne.addVersion(version);

        Item yvItemTwo = new Item("yv2", "yv2c", Publisher.YOUVIEW);
        Version version1 = new Version();
        Broadcast broadcast1 = new Broadcast("bbcone", DateTime.now(), DateTime.now());
        version1.addBroadcast(broadcast1);
        yvItemTwo.addVersion(version1);
        
        DateTime now = new DateTime().withZone(DateTimeZone.UTC);
        
        ScheduleChannel schChannel1 = new ScheduleChannel(bbcOne, ImmutableList.of(yvItemOne, yvItemTwo));
        LocalDate today = new LocalDate();
        LocalDate tomorrow = today.plusDays(1);
        Schedule schedule1 = new Schedule(ImmutableList.of(schChannel1), new Interval(today.toDateTimeAtStartOfDay(), tomorrow.toDateTimeAtStartOfDay()));
        
        ScheduleResolver resolver = scheduleResolver(schedule1);
        ResolvedContent yv1 = ResolvedContent.builder().put("yv1", yvItemOne).build();
        ResolvedContent yv2 = ResolvedContent.builder().put("yv2", yvItemTwo).build();
        when(contentResolver.findByCanonicalUris(ImmutableSet.of("yv1"))).thenReturn(yv1);
        when(contentResolver.findByCanonicalUris(ImmutableSet.of("yv2"))).thenReturn(yv2);
        ScheduleEquivalenceUpdateTask.builder()
            .withBack(0)
            .withForward(0)
            .withContentResolver(contentResolver)
            .withPublishers(ImmutableList.of(Publisher.YOUVIEW))
            .withChannelsSupplier(Suppliers.ofInstance((Iterable<Channel>)ImmutableList.of(bbcOne)))
            .withScheduleResolver(resolver)
            .withUpdater(updater)
            .build().run();
        

        verify(updater).updateEquivalences(eq(yvItemOne), any(OwlTelescopeReporter.class));
        verify(updater).updateEquivalences(eq(yvItemTwo), any(OwlTelescopeReporter.class));
    }

}
