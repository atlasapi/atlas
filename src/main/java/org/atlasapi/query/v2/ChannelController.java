package org.atlasapi.query.v2;

import static com.google.common.collect.Iterables.transform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.base.MoreOrderings;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

@Controller
public class ChannelController extends BaseController<Iterable<Channel>> {

    private static final Splitter SPLIT_ON_COMMA = Splitter.on(',');

    private static final ImmutableSet<Annotation> validAnnotations = ImmutableSet.<Annotation>builder()
        .add(Annotation.CHANNEL_GROUPS)
        .add(Annotation.HISTORY)
        .add(Annotation.PARENT)
        .add(Annotation.VARIATIONS)
        .add(Annotation.RELATED_LINKS)
        .build();

    private static final AtlasErrorSummary NOT_FOUND = new AtlasErrorSummary(new NullPointerException())
        .withMessage("No such Channel exists")
        .withMessage("Channel not found")
        .withStatusCode(HttpStatusCode.NOT_FOUND);

    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException())
        .withMessage("You require an API key to view this data")
        .withErrorCode("Api Key required")
        .withStatusCode(HttpStatusCode.FORBIDDEN);

    private static final AtlasErrorSummary BAD_ANNOTATION = new AtlasErrorSummary(new NullPointerException())
        .withMessage("Invalid annotation specified. Valid annotations are: " + Joiner.on(',').join(Iterables.transform(validAnnotations, Annotation.TO_KEY)))
        .withErrorCode("Invalid annotation")
        .withStatusCode(HttpStatusCode.BAD_REQUEST);

    private static final SelectionBuilder SELECTION_BUILDER = Selection.builder().withMaxLimit(100).withDefaultLimit(10);
    private static final Splitter CSV_SPLITTER = SPLIT_ON_COMMA.trimResults().omitEmptyStrings();
    private static final String TITLE = "title";
    private static final String TITLE_REVERSE = "title.reverse";

    private final NumberToShortStringCodec codec;
    private final QueryParameterAnnotationsExtractor annotationExtractor;
    private final ChannelResolver channelResolver;
    private final ChannelWriteController channelWriteController;

    public ChannelController(
            ApplicationConfigurationFetcher configFetcher,
            AdapterLog log,
            AtlasModelWriter<Iterable<Channel>> outputter,
            ChannelResolver channelResolver,
            NumberToShortStringCodec codec,
            ChannelWriteController channelWriteController
    ) {
        super(configFetcher, log, outputter);
        this.channelResolver = checkNotNull(channelResolver);
        this.codec = checkNotNull(codec);
        this.annotationExtractor = new QueryParameterAnnotationsExtractor();
        this.channelWriteController = checkNotNull(channelWriteController);
    }

    @RequestMapping(value={"/3.0/channels.*", "/channels.*"}, method = RequestMethod.GET)
    public void listChannels(HttpServletRequest request, HttpServletResponse response,
            @RequestParam(value = "platforms", required = false) String platformKey,
            @RequestParam(value = "regions", required = false) String regionKeys,
            @RequestParam(value = "broadcaster", required = false) String broadcasterKey,
            @RequestParam(value = "media_type", required = false) String mediaTypeKey,
            @RequestParam(value = "available_from", required = false) String availableFromKey,
            @RequestParam(value = "order_by", required = false) String orderBy,
            @RequestParam(value = "genres", required = false) String genresString,
            @RequestParam(value = "advertised", required = false) String advertiseFromKey,
            @RequestParam(value = "publisher", required = false) String publisherKey,
            @RequestParam(value = "uri", required = false) String uriKey,
            @RequestParam(value = "type", required = false) String channelType
    ) throws IOException {
        try {
            final ApplicationConfiguration appConfig;
            try {
                appConfig = appConfig(request);
            } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }

            Selection selection = SELECTION_BUILDER.build(request);

            ChannelQuery query = constructQuery(
                    platformKey,
                    regionKeys,
                    broadcasterKey,
                    mediaTypeKey,
                    availableFromKey,
                    genresString,
                    advertiseFromKey,
                    publisherKey,
                    uriKey,
                    channelType
            );

            Iterable<Channel> channels = channelResolver.allChannels(query);

            // TODO This is expensive!
            Optional<Ordering<Channel>> ordering = ordering(orderBy);
            if (ordering.isPresent()) {
                channels = ordering.get().immutableSortedCopy(channels);
            }

            channels = selection.applyTo(Iterables.filter(
                channels,
                new Predicate<Channel>() {
                    @Override
                    public boolean apply(Channel input) {
                        return appConfig.isEnabled(input.getSource());
                    }
                }));

            Optional<Set<Annotation>> annotations = annotationExtractor.extract(request);
            if (annotations.isPresent() && !validAnnotations(annotations.get())) {
                errorViewFor(request, response, BAD_ANNOTATION);
            } else {
                modelAndViewFor(request, response, channels, appConfig);
            }
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    @RequestMapping(value={"/3.0/channels/{id}.*", "/channels/{id}.*"}, method = RequestMethod.GET)
    public void listChannel(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("id") String id) throws IOException {
        try {
            Maybe<Channel> possibleChannel = channelResolver.fromId(codec.decode(id).longValue());
            if (possibleChannel.isNothing()) {
                errorViewFor(request, response, NOT_FOUND);
            } else {
                ApplicationConfiguration appConfig = appConfig(request);
                if (!appConfig.isEnabled(possibleChannel.requireValue().getSource())) {
                    outputter.writeError(request, response, FORBIDDEN.withMessage("Channel " + id + " not available"));
                    return;
                }

                Optional<Set<Annotation>> annotations = annotationExtractor.extract(request);
                if (annotations.isPresent() && !validAnnotations(annotations.get())) {
                    errorViewFor(request, response, BAD_ANNOTATION);
                } else {
                    modelAndViewFor(request, response, ImmutableList.of(possibleChannel.requireValue()), appConfig);
                }
            }
        } catch (IllegalArgumentException e) {
            response.sendError(HttpStatusCode.BAD_REQUEST.code(), e.getMessage());
        } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException e) {
            response.sendError(HttpStatusCode.FORBIDDEN.code(), e.getMessage());
        }
    }

    @RequestMapping(value={"/3.0/channels.*", "/channels.*"}, method = RequestMethod.POST)
    public void postChannel(HttpServletRequest request, HttpServletResponse response) {
        channelWriteController.postChannel(request, response);
    }

    private boolean validAnnotations(Set<Annotation> annotations) {
        return validAnnotations.containsAll(annotations);
    }

    private ChannelQuery constructQuery(
            String platformId,
            String regionIds,
            String broadcasterKey,
            String mediaTypeKey,
            String availableFromKey,
            String genresString,
            String advertiseFromKey,
            String publisherKey,
            String uri,
            String channelType
    ) {
        ChannelQuery.Builder query = ChannelQuery.builder();

        Set<Long> channelGroups = getChannelGroups(platformId, regionIds);
        if (!channelGroups.isEmpty()) {
            query.withChannelGroups(channelGroups);
        }

        if (!Strings.isNullOrEmpty(broadcasterKey)) {
            query.withBroadcaster(Publisher.fromKey(broadcasterKey).requireValue());
        }

        if (!Strings.isNullOrEmpty(mediaTypeKey)) {
            query.withMediaType(MediaType.valueOf(mediaTypeKey.toUpperCase()));
        }

        if (!Strings.isNullOrEmpty(availableFromKey)) {
            query.withAvailableFrom(Publisher.fromKey(availableFromKey).requireValue());
        }

        if (!Strings.isNullOrEmpty(genresString)) {
            Iterable<String> genres = SPLIT_ON_COMMA.split(genresString);
            query.withGenres(ImmutableSet.copyOf(genres));
        }

        if (!Strings.isNullOrEmpty(advertiseFromKey)) {
            query.withAdvertisedOn(DateTime.now(DateTimeZone.UTC));
        }

        if (!Strings.isNullOrEmpty(publisherKey)) {
            query.withPublisher(Publisher.fromKey(publisherKey).requireValue());
        }

        if (!Strings.isNullOrEmpty(uri)) {
            query.withUri(uri);
        }
        return query.build();
    }

    private Optional<Ordering<Channel>> ordering(String orderBy) {
        if (!Strings.isNullOrEmpty(orderBy)) {
            if (orderBy.equals(TITLE)) {
                return Optional.of(MoreOrderings.transformingOrdering(TO_ORDERING_TITLE));
            } else if (orderBy.equals(TITLE_REVERSE)) {
                return Optional.of(MoreOrderings.transformingOrdering(TO_ORDERING_TITLE, Ordering.<String>natural().reverse()));
            }
        }

        return Optional.absent();
    }

    private static final Function<Channel, String> TO_ORDERING_TITLE = new Function<Channel, String>() {
        @Override
        public String apply(Channel input) {
            return Strings.nullToEmpty(input.getTitle());
        }
    };

    private Set<Long> getChannelGroups(String platformId, String regionIds) {
        Builder<Long> channelGroups = ImmutableSet.builder();
        if (platformId != null) {
            channelGroups.addAll(transform(CSV_SPLITTER.split(platformId), toDecodedId));
        }
        if (regionIds != null) {
            channelGroups.addAll(transform(CSV_SPLITTER.split(regionIds), toDecodedId));
        }
        return channelGroups.build();
    }

    private final Function<String, Long> toDecodedId = new Function<String, Long>() {

        @Override
        public Long apply(String input) {
            return codec.decode(input).longValue();
        }
    };
}
