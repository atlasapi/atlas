package org.atlasapi.query.v2;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.ApplicationAccessRole;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.webapp.query.DateTimeInQueryParser;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkArgument;

@Controller
public class ScheduleController extends BaseController<Iterable<ScheduleChannel>> {

    private static final Range<Integer> COUNT_RANGE = Range.closed(1, 10);

    private static final Duration MAX_SCHEDULE_REQUEST_DURATION = Duration.standardDays(1);
    private static final PeriodFormatter PERIOD_FORMATTER = PeriodFormat.getDefault();

    private final ScheduleResolver scheduleResolver;
    private final ChannelResolver channelResolver;

    private final Application defaultApplication;

    private final DateTimeInQueryParser dateTimeInQueryParser = new DateTimeInQueryParser();
    private final NumberToShortStringCodec idCodec = new SubstitutionTableNumberCodec();

    public ScheduleController(
            ScheduleResolver scheduleResolver,
            ChannelResolver channelResolver,
            ApplicationFetcher configFetcher,
            AdapterLog log,
            AtlasModelWriter<Iterable<ScheduleChannel>> outputter,
            Application application
    ) {
        super(configFetcher, log, outputter, application);
        this.scheduleResolver = scheduleResolver;
        this.channelResolver = channelResolver;
        this.defaultApplication = application;
    }

    @RequestMapping("/3.0/schedule.*")
    public void schedule(@RequestParam(required = false) String from,
            @Nullable @RequestParam(required = false) String to,
            @Nullable @RequestParam(required = false, value = "count") String itemCount,
            @Nullable @RequestParam(required = false) String on,
            @Nullable @RequestParam(required = false) String channel,
            @Nullable @RequestParam(value = "channel_id", required = false) String channelId,
            @Nullable @RequestParam(required = false) String publisher,
            HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            Optional<Application> requestedApp;
            try {
                requestedApp = possibleApp(request);
            } catch (InvalidApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }

            DateTime fromWhen;
            DateTime toWhen = null;
            Integer count = null;

            // Fix this moment in time as "now" so all calls to the dateTime parser have the same
            // concept of when now is. This is to ensure that requests like:
            // from=now&to=now.plus.24h always resolve to exactly a 24hr interval. Otherwise
            // they could be either 24hrs or 24hrs + 1ms. The second would then trigger the filter
            // that checks schedule calls are not longer than 24hrs
            DateTime now = DateTime.now(DateTimeZone.UTC);

            if (!Strings.isNullOrEmpty(on)) {
                fromWhen = dateTimeInQueryParser.parse(on, now);
                toWhen = dateTimeInQueryParser.parse(on, now);
                checkArgument(
                        allNullOrEmpty(to, from, itemCount),
                        "'from', 'to' or 'count' cannot be provided with 'on'"
                );
            } else if (!Strings.isNullOrEmpty(to) && !Strings.isNullOrEmpty(from)) {
                fromWhen = dateTimeInQueryParser.parse(from, now);
                toWhen = dateTimeInQueryParser.parse(to, now);
                checkArgument(
                        allNullOrEmpty(on, itemCount),
                        "'on' or 'count' cannot be provided with 'from' and 'to'"
                );
            } else if (!Strings.isNullOrEmpty(from) && !Strings.isNullOrEmpty(itemCount)) {
                fromWhen = dateTimeInQueryParser.parse(from, now);
                count = Integer.parseInt(itemCount);
                checkArgument(
                        COUNT_RANGE.contains(count),
                        "'count' must be in range %s",
                        COUNT_RANGE
                );
                checkArgument(
                        allNullOrEmpty(on, to),
                        "'on' or 'to' cannot be provided with 'from' and 'count'"
                );
            } else {
                throw new IllegalArgumentException("You must supply either 'on' "
                        + "or 'from' and 'to'"
                        + "or 'from' and 'count'");
            }

            Application application = requestedApp.orElse(defaultApplication);

            if (toWhen != null) {
                checkRequestedInterval(application, fromWhen, toWhen);
            }

            boolean publishersSupplied = !Strings.isNullOrEmpty(publisher);

            Set<Publisher> publishers = getPublishers(publisher, application);

            if (publishers.isEmpty()) {
                throw new IllegalArgumentException(
                        "You must specify at least one publisher that you have permission to view");
            }

            if (Strings.isNullOrEmpty(channelId) == Strings.isNullOrEmpty(channel)) {
                throw new IllegalArgumentException(
                        "You must specify exactly one of channel and channel_id");
            }
            Set<Channel> channels = Strings.isNullOrEmpty(channel)
                                    ? channelsFromIds(channelId)
                                    : channelsFromKeys(channel);
            if (channels.isEmpty()) {
                throw new IllegalArgumentException(
                        "You must specify at least one channel that exists using the channel or channel_id parameter");
            }

            Application mergeConfig = !publishersSupplied ? application : null;
            Schedule schedule;
            if (count != null) {
                schedule = scheduleResolver.schedule(
                        fromWhen,
                        count,
                        channels,
                        publishers,
                        com.google.common.base.Optional.fromNullable(mergeConfig)
                );
            } else {
                schedule = scheduleResolver.schedule(
                        fromWhen,
                        toWhen,
                        channels,
                        publishers,
                        com.google.common.base.Optional.fromNullable(mergeConfig)
                );
            }
            modelAndViewFor(request, response, schedule.scheduleChannels( ), application);
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

    private Set<Publisher> getPublishers(String publisher, Application application) {
        if (!Strings.isNullOrEmpty(publisher)) {
            return Sets.intersection(
                    publishersFrom(publisher),
                    application.getConfiguration().getEnabledReadSources()
            );
        }
        if (application.getConfiguration().isPrecedenceEnabled()) {
            return ImmutableSet.of(
                    application.getConfiguration()
                            .getReadPrecedenceOrdering()
                            .sortedCopy(application.getConfiguration().getEnabledReadSources())
                            .get(0)
            );
        }
        throw new IllegalArgumentException("Need precedence enabled");
    }

    private Set<Channel> channelsFromIds(String channelId) {
        return ImmutableSet.copyOf(Iterables.transform(Iterables.filter(Iterables.transform(
                URI_SPLITTER.split(channelId),
                input -> channelResolver.fromId(idCodec.decode(input).longValue())
        ), Maybe.HAS_VALUE), Maybe.requireValueFunction()));
    }

    private Set<Channel> channelsFromKeys(String channelString) {
        ImmutableSet.Builder<Channel> channels = ImmutableSet.builder();
        for (String channelKey : URI_SPLITTER.split(channelString)) {
            Maybe<Channel> channel = channelResolver.fromKey(channelKey);
            if (channel.hasValue()) {
                channels.add(channel.requireValue());
            }
        }
        return channels.build();
    }

    private void checkRequestedInterval(
            Application application,
            DateTime fromWhen,
            DateTime toWhen
    ) {
        // These API keys are allowed to make big schedule requests
        if (application.getAccessRoles().hasRole(ApplicationAccessRole.SUNSETTED_API_FEATURES_ACCESS.getRole())) {
            return;
        }

        Interval interval = new Interval(fromWhen, toWhen);
        if (interval.toDuration().isLongerThan(MAX_SCHEDULE_REQUEST_DURATION)) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Schedule requests for more than {0} are not allowed",
                    PERIOD_FORMATTER.print(MAX_SCHEDULE_REQUEST_DURATION.toPeriod())
            ));
        }
    }
}
