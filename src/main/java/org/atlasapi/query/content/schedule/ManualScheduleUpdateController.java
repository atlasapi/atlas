package org.atlasapi.query.content.schedule;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleEntryBuilder;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.joda.time.Duration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@Controller
public class ManualScheduleUpdateController {
    
    private final ScheduleWriter scheduleWriter;
    private final ContentResolver resolver;
    private ExecutorService executor;
    private ScheduleEntryBuilder scheduleEntryBuilder;

    public ManualScheduleUpdateController(ScheduleWriter scheduleWriter, ContentResolver resolver, ChannelResolver channelResolver) {
        this.scheduleWriter = scheduleWriter;
        this.resolver = resolver;
        this.scheduleEntryBuilder = new ScheduleEntryBuilder(channelResolver, Duration.standardSeconds(Long.MAX_VALUE/1000));
        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Manual Schedule Update Thread").build());
    }
    
    @RequestMapping(value = "/system/schedule/update", method = RequestMethod.POST)
    public void runUpdate(HttpServletResponse response, @RequestParam(value = "uris", required = true) String uris) throws IOException {
        
        final Iterable<String> splitUris = Splitter.on(",").split(uris);
        
        Future<?> result = executor.submit(new Runnable() {
            @Override
            public void run() {
                ResolvedContent resolvedContent = resolver.findByCanonicalUris(splitUris);
                Iterable<Item> items = Iterables.filter(resolvedContent.getAllResolvedResults(),Item.class);
                Map<String, ScheduleEntry> scheduleEntries = scheduleEntryBuilder.toScheduleEntries(items);
                for (ScheduleEntry entry : scheduleEntries.values()) {
                    scheduleWriter.writeCompleteEntry(entry);
                }
            }
        });
        
        try {
            result.get();
            response.setStatus(200);
            response.setContentLength(0);
        } catch (Exception e) {
            response.setStatus(500);
            e.printStackTrace(response.getWriter());
        }
        
    }
    
}
