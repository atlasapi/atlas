package org.atlasapi.remotesite.metabroadcast.picks;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.bbc.nitro.ChannelDay;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.client.TelescopeReporterName;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

@Controller
public class PicksChannelDayUpdateController {

    private final NumberToShortStringCodec channelIdCodec = new SubstitutionTableNumberCodec();
    private final ChannelResolver channelResolver;
    private final PicksDayUpdater picksDayUpdater;
    
    public PicksChannelDayUpdateController(ChannelResolver channelResolver, PicksDayUpdater picksDayUpdater) {
        this.channelResolver = checkNotNull(channelResolver);
        this.picksDayUpdater = checkNotNull(picksDayUpdater);
    }
    
    @RequestMapping(value="/system/update/picks/{dateString}/{channelId}",  method=RequestMethod.POST)
    public void updatePicks(@PathVariable String dateString, @PathVariable String channelId, 
            HttpServletResponse response) throws Exception {
        
        Maybe<Channel> channel = channelResolver.fromId(channelIdCodec.decode(channelId).longValue());
        LocalDate date = ISODateTimeFormat.dateParser().parseLocalDate(dateString);
        ChannelDay channelDay = new ChannelDay(channel.requireValue(), date);

        OwlTelescopeReporter telescope = OwlTelescopeReporter.create(OwlTelescopeReporters.BBC_NITRO_INGEST_PICKS, Event.Type.INGEST);
        telescope.startReporting();
        picksDayUpdater.process(channelDay, telescope);
        response.setStatus(200);
        telescope.endReporting();
    }
}
