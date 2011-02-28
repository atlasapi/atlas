package org.atlasapi.remotesite.channel4.epg;

import static org.atlasapi.media.entity.Channel.CHANNEL_FOUR;

import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BroadcastTrimmerTest extends TestCase {
    private final Mockery context = new Mockery();
    
    private final ScheduleResolver scheduleResolver = context.mock(ScheduleResolver.class);
    private final ContentWriter contentWriter = context.mock(ContentWriter.class);
    private final Channel channel = Channel.CHANNEL_FOUR;
    private final Set<Channel> channels = ImmutableSet.of(channel);
    private final Set<Publisher> publishers = ImmutableSet.of(Publisher.C4);

    public void testTrimBroadcasts() {
        final Schedule schedule = Schedule.fromChannelMap(channelMap(), new Interval(100, 200));
        
        context.checking(new Expectations(){{
            oneOf(scheduleResolver).schedule(with(any(DateTime.class)), with(any(DateTime.class)), with(channels), with(publishers)); will(returnValue(schedule));
            one(contentWriter).createOrUpdate(with(trimmedItem()));
        }});
        
        AdapterLog log = new NullAdapterLog();
        
        BroadcastTrimmer trimmer = new BroadcastTrimmer(Publisher.C4, scheduleResolver, contentWriter, log);
        
        Interval scheduleInterval = new Interval(100, 200);
        trimmer.trimBroadcasts(scheduleInterval, CHANNEL_FOUR, ImmutableSet.of("c4:1234"));
        
    }

    private Matcher<Item> trimmedItem() {
        return new TypeSafeMatcher<Item>() {
            @Override
            public void describeTo(Description desc) {
                desc.appendText("trimmed item with broadcast with id c4:1234");
            }

            @Override
            public boolean matchesSafely(Item item) {
                Set<Broadcast> broadcasts = Iterables.getOnlyElement(item.getVersions()).getBroadcasts();
                if(broadcasts.size() != 2) {
                    return false;
                }
                return check(Iterables.get(broadcasts, 0)) && check(Iterables.get(broadcasts, 0));
            }

            private boolean check(Broadcast broadcast) {
                return broadcast.getId().equals("c4:1234") && broadcast.isActivelyPublished() || broadcast.getId().equals("c4:2234") && !broadcast.isActivelyPublished();
            }
        };
    }
    
    private Iterable<? extends Item> buildItems() {
        Item item = new Item("testUri", "testCurie", Publisher.C4);
        Version version = new Version();
        
        Broadcast retain = new Broadcast(Channel.CHANNEL_FOUR.uri(), new DateTime(105), new DateTime(120)).withId("c4:1234");
        retain.setIsActivelyPublished(true);
        Broadcast remove = new Broadcast(Channel.CHANNEL_FOUR.uri(), new DateTime(150), new DateTime(165)).withId("c4:2234");
        remove.setIsActivelyPublished(true);
        //this gets stripped by the Schedule object.
        Broadcast outsideInterval = new Broadcast(Channel.CHANNEL_FOUR.uri(), new DateTime(205), new DateTime(620)).withId("c4:6234");
        
        version.setBroadcasts(ImmutableSet.of(retain, outsideInterval, remove));
        item.addVersion(version);
        
        return ImmutableSet.of(item);
    }
    
    private Map<Channel, List<Item>> channelMap() {
        Map<Channel, List<Item>> channelMap = Maps.newHashMap();
        channelMap.put(Channel.CHANNEL_FOUR, Lists.newArrayList(buildItems()));
        return channelMap;
    }
}
