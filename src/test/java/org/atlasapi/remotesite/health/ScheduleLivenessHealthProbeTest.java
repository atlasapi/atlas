package org.atlasapi.remotesite.health;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import java.util.List;

import junit.framework.TestCase;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.health.ProbeResult;

import org.joda.time.base.BaseSingleFieldPeriod;

public class ScheduleLivenessHealthProbeTest extends TestCase {
	private static final int TWELVE_POINT_FIVE_DAYS_IN_HOURS = 300;
	private DateTime startTimeForScheduleTest;
	private DateTime endTimeForScheduleTest;
	
    private static final Channel BBC_ONE = new Channel(Publisher.METABROADCAST, "BBC One", "bbcone", false, MediaType.AUDIO, "http://www.bbc.co.uk/bbcone");
    private static final Channel BBC_TWO = new Channel(Publisher.METABROADCAST, "BBC Two", "bbctwo", false, MediaType.AUDIO, "http://www.bbc.co.uk/bbctwo");
    private static final Channel ITV1_LONDON = new Channel(Publisher.METABROADCAST, "ITV 1 London", "itv1london", false, MediaType.AUDIO, "http://www.itv.com");

	public void setUp() {
		this.startTimeForScheduleTest = new DateTime().plusHours(TWELVE_POINT_FIVE_DAYS_IN_HOURS);
		this.endTimeForScheduleTest = startTimeForScheduleTest.plusHours(12);
	}
	
	@SuppressWarnings("unchecked")
	public void testHasSchedule() throws Exception {
		
		Interval dummyInterval = new Interval(startTimeForScheduleTest, endTimeForScheduleTest);
		Item item = new Item();
		Schedule schedule = new Schedule(ImmutableList.of(
				new ScheduleChannel(BBC_ONE, ImmutableList.<Item>of(item)),
				new ScheduleChannel(BBC_TWO, ImmutableList.<Item>of(item))
				), dummyInterval);
		
		DummyScheduleResolver dummySchedule = new DummyScheduleResolver(schedule);
		
		ScheduleLivenessHealthProbe probe = new ScheduleLivenessHealthProbe(dummySchedule,
				ImmutableList.of(BBC_ONE, BBC_TWO), Publisher.PA);
		ProbeResult result = probe.probe();
		
		assertThat(result.isFailure(), is(false));
		assertThat(Minutes.minutesBetween(dummySchedule.requestedStartTime, startTimeForScheduleTest), lessThan((BaseSingleFieldPeriod) Minutes.minutes(2)));
		assertThat(Minutes.minutesBetween(dummySchedule.requestedEndTime, endTimeForScheduleTest), lessThan((BaseSingleFieldPeriod) Minutes.minutes(2)));
	}
	
	@SuppressWarnings("unchecked")
	public void testNoSchedule() throws Exception {
		Interval dummyInterval = new Interval(new DateTime().plusHours(TWELVE_POINT_FIVE_DAYS_IN_HOURS), new DateTime().plusHours(TWELVE_POINT_FIVE_DAYS_IN_HOURS).plusHours(12));
		Item item = new Item();
		Schedule schedule = new Schedule(ImmutableList.of(
				new ScheduleChannel(BBC_ONE, ImmutableList.<Item>of(item)),
				new ScheduleChannel(BBC_TWO, ImmutableList.<Item>of(item))
				), dummyInterval);
		
		DummyScheduleResolver dummySchedule = new DummyScheduleResolver(schedule);
		
		ScheduleLivenessHealthProbe probe = new ScheduleLivenessHealthProbe(dummySchedule,
				ImmutableList.of(BBC_ONE, BBC_TWO, ITV1_LONDON), Publisher.PA);
		ProbeResult result = probe.probe();
		
		assertThat(result.isFailure(), is(true));
		assertThat(Minutes.minutesBetween(dummySchedule.requestedStartTime, startTimeForScheduleTest), lessThan((BaseSingleFieldPeriod)Minutes.minutes(2)));
		assertThat(Minutes.minutesBetween(dummySchedule.requestedEndTime, endTimeForScheduleTest), lessThan((BaseSingleFieldPeriod)Minutes.minutes(2)));
	}
	
	@SuppressWarnings("unchecked")
	public void testNoItemsInSchedule() throws Exception {
		Interval dummyInterval = new Interval(new DateTime().plusHours(TWELVE_POINT_FIVE_DAYS_IN_HOURS), new DateTime().plusHours(TWELVE_POINT_FIVE_DAYS_IN_HOURS).plusHours(12));
		Item item = new Item();
		Schedule schedule = new Schedule(ImmutableList.of(
				new ScheduleChannel(BBC_ONE, ImmutableList.<Item>of()),
				new ScheduleChannel(BBC_TWO, ImmutableList.<Item>of(item))
				), dummyInterval);
		
		DummyScheduleResolver dummySchedule = new DummyScheduleResolver(schedule);
		
		ScheduleLivenessHealthProbe probe = new ScheduleLivenessHealthProbe(dummySchedule,
				ImmutableList.of(BBC_ONE, BBC_TWO), Publisher.PA);
		
		ProbeResult result = probe.probe();
		
		assertThat(result.isFailure(), is(true));
		assertThat(Minutes.minutesBetween(dummySchedule.requestedStartTime, startTimeForScheduleTest), lessThan((BaseSingleFieldPeriod)Minutes.minutes(2)));
		assertThat(Minutes.minutesBetween(dummySchedule.requestedEndTime, endTimeForScheduleTest), lessThan((BaseSingleFieldPeriod)Minutes.minutes(2)));
	}
	
	protected static class DummyScheduleResolver implements ScheduleResolver {

		public DateTime requestedStartTime;
		public DateTime requestedEndTime;
		
		private Schedule schedule;
		
		public DummyScheduleResolver(Schedule dummySchedule) {
			this.schedule = dummySchedule;
		}

		@Override
		public Schedule unmergedSchedule(DateTime from, DateTime to,
				Iterable<Channel> channels, Iterable<Publisher> publisher) {
			
			this.requestedStartTime = from;
			this.requestedEndTime = to;
			
			final List<Channel> channelList = ImmutableList.copyOf(channels);
			
			return new Schedule(ImmutableList.copyOf(Iterables.filter(schedule.scheduleChannels(), new Predicate<ScheduleChannel>() {

				@Override
				public boolean apply(ScheduleChannel input) {
					return channelList.contains(input.channel());
				}
				
				
			})), schedule.interval());
			
		}
		
		@Override
		public Schedule schedule(DateTime from, DateTime to, Iterable<Channel> channels,
		        Iterable<Publisher> publisher, Optional<ApplicationConfiguration> mergeConfig) {
		    return null;
		}
		
		@Override
		public Schedule schedule(DateTime from, int count, Iterable<Channel> channels,
		        Iterable<Publisher> publisher, Optional<ApplicationConfiguration> mergeConfig) {
		    return null;
		}
		
	}
}
