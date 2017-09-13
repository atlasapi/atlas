package org.atlasapi.remotesite.metabroadcast.picks;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.bbc.nitro.ChannelDay;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.base.Throwables;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

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

        //this telescope is created because it is needed as an argument, but never used.
        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                        OwlTelescopeReporters.PICKS_CONTENT_GROUP_UPDATER,
                        Event.Type.INGEST
        );

        try {
            Maybe<Channel> channel = channelResolver.fromId(channelIdCodec.decode(channelId).longValue());
            LocalDate date = ISODateTimeFormat.dateParser().parseLocalDate(dateString);
            ChannelDay channelDay = new ChannelDay(channel.requireValue(), date);

            picksDayUpdater.process(channelDay, telescope);
            response.setStatus(HttpStatusCode.OK.code());
        } catch (Exception e) {
            String stack = Throwables.getStackTraceAsString(e);
            response.setStatus(HttpStatusCode.SERVER_ERROR.code());
            response.setContentLength(stack.length());
            response.getWriter().write(stack);
        }
    }
}
