package org.atlasapi.equiv.update.tasks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.ChannelSchedule;
import org.atlasapi.media.util.ItemAndBroadcast;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


public class ScheduleEquivalenceUpdateTaskTest {

    @SuppressWarnings("unchecked") 
    private final EquivalenceUpdater<Content> updater = mock(EquivalenceUpdater.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);

    // TODO make this take multiple schedules
    private final ScheduleResolver scheduleResolver(final Schedule schedule) {
        return new ScheduleResolver() {
            
            @Override
            public Schedule schedule(DateTime from, DateTime to, Iterable<Channel> channels,
                    Iterable<Publisher> publisher, Optional<ApplicationSources> mergeConfig) {
                return schedule;
            }
        };
    };
    
    @Test
    public void testUpdateScheduleEquivalences() {
        
        Channel bbcOne = new Channel(Publisher.METABROADCAST, "BBC One", "bbcone", false, MediaType.VIDEO, "bbconeuri");
        
        DateTime now = new DateTime().withZone(DateTimeZone.UTC);
        
        Item yvItemOne = new Item("yv1", "yv1c", Publisher.YOUVIEW);
        yvItemOne.setId(1);
        Broadcast yvBroadcastOne = new Broadcast(bbcOne.getCanonicalUri(), now, now);
        ItemAndBroadcast one = new ItemAndBroadcast(yvItemOne, yvBroadcastOne);

        Item yvItemTwo = new Item("yv2", "yv2c", Publisher.YOUVIEW);
        yvItemTwo.setId(2);
        Broadcast yvBroadcastTwo = new Broadcast(bbcOne.getCanonicalUri(), now, now);
        ItemAndBroadcast two = new ItemAndBroadcast(yvItemTwo, yvBroadcastTwo);
        
        LocalDate today = new LocalDate();
        LocalDate tomorrow = today.plusDays(1);
        Interval interval = new Interval(today.toDateTimeAtStartOfDay(), tomorrow.toDateTimeAtStartOfDay());
        ChannelSchedule schChannel1 = new ChannelSchedule(bbcOne, interval, ImmutableList.of(one, two));
        Schedule schedule1 = new Schedule(ImmutableList.of(schChannel1), interval);
        
        ScheduleResolver resolver = scheduleResolver(schedule1);
        
        ScheduleEquivalenceUpdateTask.builder()
            .withBack(0)
            .withForward(0)
            .withPublishers(ImmutableList.of(Publisher.YOUVIEW))
            .withChannels(ImmutableList.of(bbcOne))
            .withScheduleResolver(resolver)
            .withUpdater(updater)
            .build().run();
        
        
        verify(updater).updateEquivalences(yvItemOne);
        verify(updater).updateEquivalences(yvItemTwo);
    }

}
