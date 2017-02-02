package org.atlasapi.remotesite.health;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;

public class ScheduleLivenessHealthProbe implements HealthProbe {

	private ScheduleResolver scheduleResolver;
	private Set<Channel> channels;
	
	private static final int TWELVE_POINT_FIVE_DAYS_IN_HOURS = 300;
	public static final String SCHEDULE_HEALTH_PROBE_SLUG = "schedule-liveness";
	
	private final Log log = LogFactory.getLog(getClass());
	private Publisher publisher;
	
	public ScheduleLivenessHealthProbe(ScheduleResolver scheduleResolver, Iterable<Channel> channels, Publisher publisher) {
		this.scheduleResolver = scheduleResolver;
		this.channels = ImmutableSet.copyOf(channels);
		this.publisher = publisher;
	}

	@Override
	public ProbeResult probe() throws Exception {
	    
		ProbeResult result = null;
		try {
			result = new ProbeResult(title());
			if (channels.isEmpty()) {
			    result.addInfo("Channels", "not configured");
			}
			
			DateTime startTime = new DateTime().plusHours(TWELVE_POINT_FIVE_DAYS_IN_HOURS);
			DateTime endTime = startTime.plusHours(12);
			
			for(Channel channel : channels) {
				try {
					Schedule schedule = scheduleResolver.unmergedSchedule(startTime, endTime, ImmutableSet.of(channel), ImmutableSet.of(publisher));
					int itemCount = Iterables.getOnlyElement(schedule.scheduleChannels()).items().size();
							
					result.add(channel.getTitle(), String.format("%d items in schedule from %s to %s", itemCount, startTime.toString("dd/MM/yy HH:mm"), endTime.toString("dd/MM/yy HH:mm")), itemCount > 0);
				}
				catch(Exception e) {
					result.add(channel.getTitle(), "Exception processing channel", false);
					log.error("Exception processing channel " + channel.getTitle(), e);
				}
			}
		}
		catch(Exception e) {
			log.error("Exception in schedule liveness probe", e);
			Throwables.propagate(e);
		}
		
		return result;
	}

	@Override
	public String title() {
		return "Schedule liveness";
	}

	@Override
	public String slug() {
		return SCHEDULE_HEALTH_PROBE_SLUG;
	}

}
