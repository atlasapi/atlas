package org.atlasapi.remotesite.bbc.nitro;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.bbc.ion.BbcIonServices;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Throwables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;

/**
 * <p>
 * Controller providing an end-point to update a single channel-day of Nitro
 * content.
 * </p>
 * 
 * <p>
 * <strong>POST</strong> to {@code /system/bbc/nitro/update/:service/:yyyyMMdd}
 * </p>
 */
@Controller
public class ScheduleDayUpdateController {

    private final DateTimeFormatter dateFormat = ISODateTimeFormat.basicDate().withZone(DateTimeZones.UTC);
    private final ChannelDayProcessor processor;
    private ChannelResolver resolver;

    public ScheduleDayUpdateController(ChannelResolver resolver, ChannelDayProcessor processor) {
        this.resolver = resolver;
        this.processor = processor;
    }

    @RequestMapping(value="/system/bbc/nitro/update/{service}/{date}", method=RequestMethod.POST)
    public void updateScheduleDay(HttpServletResponse resp,
            @PathVariable("service") String service, @PathVariable("date") String date) throws IOException {
        
        Maybe<Channel> possibleChannel = resolver.fromUri(BbcIonServices.get(service));
        if (possibleChannel.isNothing()) {
            resp.sendError(HttpStatusCode.NOT_FOUND.code());
            return;
        }
        
        LocalDate day = null;
        try {
            day = dateFormat.parseLocalDate(date);
        } catch (IllegalArgumentException iae) {
            resp.sendError(HttpStatusCode.NOT_FOUND.code());
            return;
        }
        
        try {
            UpdateProgress progress = processor.process(new ChannelDay(possibleChannel.requireValue(), day));
            resp.setStatus(HttpStatusCode.OK.code());
            String progressMsg = progress.toString();
            resp.setContentLength(progressMsg.length());
            resp.getWriter().write(progressMsg);
        } catch (Exception e) {
            String stack = Throwables.getStackTraceAsString(e);
            resp.setStatus(HttpStatusCode.SERVER_ERROR.code());
            resp.setContentLength(stack.length());
            resp.getWriter().write(stack);
            return;
        }
    }
    
}
