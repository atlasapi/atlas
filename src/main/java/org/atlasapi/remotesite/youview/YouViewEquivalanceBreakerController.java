package org.atlasapi.remotesite.youview;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.time.DateTimeZones;

@Controller
public class YouViewEquivalanceBreakerController {

    private final DateTimeFormatter dateFormat = ISODateTimeFormat.basicDate().withZone(DateTimeZones.UTC);
    private final YouViewEquivalenceBreaker equivalenceBreaker;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public YouViewEquivalanceBreakerController(YouViewEquivalenceBreaker equivalenceBreaker) {
        this.equivalenceBreaker = checkNotNull(equivalenceBreaker);
    }
    
    @RequestMapping(value="/system/youview/orphan/{date}", method=RequestMethod.POST)
    public void breakEquivalence(HttpServletResponse response, 
            @PathVariable("date") String date) throws IOException {
        
        LocalDate day = null;
        try {
            day = dateFormat.parseLocalDate(date);
        } catch (IllegalArgumentException iae) {
            response.sendError(HttpStatusCode.NOT_FOUND.code());
            return;
        }
        final DateTime startOfDay = day.toDateTimeAtStartOfDay();
        executor.submit(new Runnable() {

            @Override
            public void run() {
                equivalenceBreaker.orphanItems(startOfDay, startOfDay.plusDays(1));
                
            }
        });
        response.setStatus(HttpStatusCode.ACCEPTED.code());
    }
}
