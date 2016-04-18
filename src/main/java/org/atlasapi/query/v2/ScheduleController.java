package org.atlasapi.query.v2;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.joda.time.DateTime;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser;

@Controller
public class ScheduleController extends BaseController<Iterable<ScheduleChannel>> {

    private static final Range<Integer> COUNT_RANGE = Range.closed(1, 10);
    
    private final ScheduleResolver scheduleResolver;
    private final ChannelResolver channelResolver;

    private final ApplicationConfiguration defaultConfig
        = ApplicationConfiguration.forNoApiKey();
	
	private final DateTimeInQueryParser dateTimeInQueryParser = new DateTimeInQueryParser();
    private final NumberToShortStringCodec idCodec = new SubstitutionTableNumberCodec();

    
    public ScheduleController(ScheduleResolver scheduleResolver, ChannelResolver channelResolver, ApplicationConfigurationFetcher configFetcher, AdapterLog log, AtlasModelWriter<Iterable<ScheduleChannel>> outputter) {
        super(configFetcher, log, outputter);
        this.scheduleResolver = scheduleResolver;
        this.channelResolver = channelResolver;
    }

    @RequestMapping("/3.0/schedule.*")
    public void schedule(@RequestParam(required=false) String from, 
            @RequestParam(required=false) String to, 
            @RequestParam(required=false, value="count") String itemCount,
            @RequestParam(required=false) String on, 
            @RequestParam(required=false) String channel, @RequestParam(value="channel_id",required=false) String channelId, 
            @RequestParam(required=false) String publisher, 
            HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            Maybe<ApplicationConfiguration> requestedConfig;
            try {
                requestedConfig = possibleAppConfig(request);
            } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }

            DateTime fromWhen = null;
            DateTime toWhen = null;
            Integer count = null; 

            if (!Strings.isNullOrEmpty(on)) {
                fromWhen = dateTimeInQueryParser.parse(on);
                toWhen = dateTimeInQueryParser.parse(on);
                checkArgument(allNullOrEmpty(to, from, itemCount), "'from', 'to' or 'count' cannot be provided with 'on'");
            } else if (!Strings.isNullOrEmpty(to) && !Strings.isNullOrEmpty(from)) {
                fromWhen = dateTimeInQueryParser.parse(from);
                toWhen = dateTimeInQueryParser.parse(to);
                checkArgument(allNullOrEmpty(on, itemCount), "'on' or 'count' cannot be provided with 'from' and 'to'");
            } else if (!Strings.isNullOrEmpty(from) && !Strings.isNullOrEmpty(itemCount)) {
                fromWhen = dateTimeInQueryParser.parse(from);
                count = Integer.parseInt(itemCount);
                checkArgument(COUNT_RANGE.contains(count), "'count' must be in range %s", COUNT_RANGE);
                checkArgument(allNullOrEmpty(on, to), "'on' or 'to' cannot be provided with 'from' and 'count'");
            } else {
                throw new IllegalArgumentException("You must supply either 'on' "
                        + "or 'from' and 'to'"
                        + "or 'from' and 'count'");
            }

            ApplicationConfiguration appConfig = requestedConfig.valueOrDefault(defaultConfig);
            
            boolean publishersSupplied = !Strings.isNullOrEmpty(publisher);

            Set<Publisher> publishers = getPublishers(publisher, appConfig);
            
            if (publishers.isEmpty()) {
                throw new IllegalArgumentException("You must specify at least one publisher that you have permission to view");
            }
            
            if (!(Strings.isNullOrEmpty(channelId) ^ Strings.isNullOrEmpty(channel))) {
                throw new IllegalArgumentException("You must specify exactly one of channel and channel_id");
            }
            Set<Channel> channels = Strings.isNullOrEmpty(channel) ? channelsFromIds(channelId) : channelsFromKeys(channel);
            if (channels.isEmpty()) {
                throw new IllegalArgumentException("You must specify at least one channel that exists using the channel or channel_id parameter");
            }
            
            ApplicationConfiguration mergeConfig = !publishersSupplied ? appConfig : null;
            Schedule schedule;
            if (count != null) {
                schedule = scheduleResolver.schedule(fromWhen, count, channels, publishers, Optional.fromNullable(mergeConfig));
            } else {
                schedule = scheduleResolver.schedule(fromWhen, toWhen, channels, publishers, Optional.fromNullable(mergeConfig));
            }
            modelAndViewFor(request, response, schedule.scheduleChannels(), appConfig);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    private boolean allNullOrEmpty(String... params) {
        for (String param : params) {
            if (!Strings.isNullOrEmpty(param)) {
                return false;
            }
        }
        return true;
    }

    private Set<Publisher> getPublishers(String publisher,
                                         ApplicationConfiguration appConfig) {
        if (!Strings.isNullOrEmpty(publisher)) {
            return Sets.intersection(publishersFrom(publisher), appConfig.getEnabledSources());
        }
        if (appConfig.precedenceEnabled()) {
            return ImmutableSet.of(appConfig.orderdPublishers().get(0));
        } 
        throw new IllegalArgumentException("Need precedence enabled");
    }

    private Set<Channel> channelsFromIds(String channelId) {
        return ImmutableSet.copyOf(Iterables.transform(Iterables.filter(Iterables.transform(URI_SPLITTER.split(channelId), new Function<String, Maybe<Channel>>() {
            @Override
            public Maybe<Channel> apply(String input) {
                return channelResolver.fromId(idCodec.decode(input).longValue());
            }
        }),Maybe.HAS_VALUE),Maybe.<Channel>requireValueFunction()));
    }

    private Set<Channel> channelsFromKeys(String channelString) {
        ImmutableSet.Builder<Channel> channels = ImmutableSet.builder();
        for (String channelKey: URI_SPLITTER.split(channelString)) {
            Maybe<Channel> channel = channelResolver.fromKey(channelKey);
            if (channel.hasValue()) {
                channels.add(channel.requireValue());
            }
        }
        return channels.build();
    }
}
