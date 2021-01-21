package org.atlasapi.query.v2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.social.exceptions.BadRequestException;
import org.apache.commons.cli.MissingArgumentException;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.input.ChannelGroupTransformer;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupType;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.query.v2.ChannelGroupFilterer.ChannelGroupFilter;
import org.atlasapi.query.v2.ChannelGroupFilterer.ChannelGroupFilter.ChannelGroupFilterBuilder;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class ChannelGroupController extends BaseController<Iterable<ChannelGroup>> {

    private static final Logger log = LoggerFactory.getLogger(ChannelGroupWriteExecutor.class);
    private static final Splitter SPLIT_ON_COMMA = Splitter.on(',');
    private static final ImmutableSet<Annotation> validAnnotations = ImmutableSet.<Annotation>builder()
            .add(Annotation.CHANNELS)
            .add(Annotation.HISTORY)
            .add(Annotation.CHANNEL_GROUPS_SUMMARY)
            .build();
    private static final AtlasErrorSummary NOT_FOUND = new AtlasErrorSummary(new NullPointerException())
        .withMessage("No such Channel Group exists")
        .withErrorCode("Channel Group not found")
        .withStatusCode(HttpStatusCode.NOT_FOUND);
    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException())
        .withMessage("You require an API key to view this data")
        .withErrorCode("Api Key required")
        .withStatusCode(HttpStatusCode.FORBIDDEN);
    private static final AtlasErrorSummary BAD_ANNOTATION = new AtlasErrorSummary(new NullPointerException())
        .withMessage("Invalid annotation specified. Valid annotations are: " + Joiner.on(',').join(
                validAnnotations.stream()
                        .map(Annotation.TO_KEY::apply)
                        .collect(Collectors.toList())
        ))
        .withErrorCode("Invalid annotation")
        .withStatusCode(HttpStatusCode.BAD_REQUEST);
    private static final AtlasErrorSummary INVALID_PARAMETER = new AtlasErrorSummary(new IllegalArgumentException())
            .withMessage("dtt_only and ip_only parameters are mutually exclusive")
            .withErrorCode("Incorrect request parameter")
            .withStatusCode(HttpStatusCode.BAD_REQUEST);

    private static final String ADVERTISED = "advertised";
    private static final String TYPE_KEY = "type";
    private static final String PLATFORM_ID_KEY = "platform_id";
    private static final String CHANNEL_GENRES_KEY = "channel_genres";
    private static final SelectionBuilder SELECTION_BUILDER = Selection.builder().withMaxLimit(50).withDefaultLimit(10);
    private static final String DTT_ONLY = "dtt_only";
    private static final String IP_ONLY = "ip_only";
    private static final String BT_FUTURE_CHANNELS = "future_channels";
    private static final String SOURCE = "source";
    /**
     * Should be OWL or DEER, but we only check if it is DEER, and if it is, we'll assume that the
     * provided ids are deer ids, and covert them to owl ids before we store them. This is because
     * the caller might only have access to deer IDs. Reminder that channels and channelgroups,
     * still use the old id format where things could use capitals etc. The parameter affects both
     * the ChannelGroup and Channels inside it.
     */
    private static final String ID_FORMAT = "id_format";
    public static final String DEER = "deer";
    public static final String OWL = "owl";

    public static final String REFRESH_CACHE = "refresh_cache";

    private final ChannelGroupFilterer filterer = new ChannelGroupFilterer();
    private final NumberToShortStringCodec oldFormatIdCodec = new SubstitutionTableNumberCodec();
    private final NumberToShortStringCodec newFormatIdCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ChannelResolver channelResolver;
    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelGroupWriteExecutor channelGroupWriteExecutor;
    private final QueryParameterAnnotationsExtractor annotationExtractor;
    private final ApplicationFetcher applicationFetcher;
    private final ChannelGroupTransformer channelGroupTransformer;
    private final ModelReader modelReader;

    private ChannelGroupController(Builder builder) {
        super(builder.applicationFetcher, builder.log, builder.atlasModelWriter, DefaultApplication.createDefault());
        this.applicationFetcher = checkNotNull(builder.applicationFetcher);
        this.channelResolver = checkNotNull(builder.channelResolver);
        this.channelGroupResolver = checkNotNull(builder.channelGroupResolver);
        this.channelGroupTransformer = checkNotNull(builder.channelGroupTransformer);
        this.channelGroupWriteExecutor = checkNotNull(builder.channelGroupWriteExecutor);
        this.modelReader = checkNotNull(builder.modelReader);
        this.annotationExtractor = new QueryParameterAnnotationsExtractor();
    }

    public static Builder builder() {
        return new Builder();
    }

    @RequestMapping(value={"/3.0/channel_groups.*", "/channel_groups.*"})
    public void listChannels(HttpServletRequest request, HttpServletResponse response,
            @RequestParam(value = TYPE_KEY, required = false) String type,
            @RequestParam(value = PLATFORM_ID_KEY, required = false) String platformId,
            @RequestParam(value = ADVERTISED, required = false) String advertised,
            @RequestParam(value = DTT_ONLY, defaultValue = "", required = false) String dttOnly,
            @RequestParam(value = IP_ONLY, defaultValue = "", required = false) String ipOnly,
            @RequestParam(value = BT_FUTURE_CHANNELS, defaultValue = "false", required = false) boolean futureChannels,
            @RequestParam(value = SOURCE, required = false) String source
    ) throws IOException {
        try {
            final Application application;
            try {
                application = application(request);
            } catch (InvalidApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }
            
            Optional<Set<Annotation>> annotations = annotationExtractor.extract(request);
            if (annotations.isPresent() && !validAnnotations(annotations.get())) {
                errorViewFor(request, response, BAD_ANNOTATION);
                return;
            }

            if (!Strings.isNullOrEmpty(dttOnly) && !Strings.isNullOrEmpty(ipOnly)) {
                errorViewFor(request, response, INVALID_PARAMETER);
                return;
            }
            
            List<ChannelGroup> channelGroups = ImmutableList.copyOf(channelGroupResolver.channelGroups());

            Selection selection = SELECTION_BUILDER.build(request);        
            channelGroups = selection.applyTo(filterer.filter(
                    channelGroups,
                    constructFilter(platformId, type, advertised)
            )
                    .stream()
                    .filter(input -> application.getConfiguration()
                            .isReadEnabled(input.getPublisher()))
                    .collect(Collectors.toList()));

            if (!Strings.isNullOrEmpty(source)) {
                if (!Publisher.fromKey(source).hasValue()) {
                    errorViewFor(
                            request,
                            response,
                            AtlasErrorSummary.forException(
                                    new IllegalArgumentException(
                                            String.format("No publisher found for key %s", source)
                                    )
                            )
                    );
                } else {
                    channelGroups = channelGroups.stream()
                            .filter(input -> input.getPublisher().key().equals(source))
                            .collect(Collectors.toList());
                }
            }

            if (!Strings.isNullOrEmpty(advertised)) {
                ImmutableList.Builder filtered = ImmutableList.builder();
                for (ChannelGroup channelGroup : channelGroups) {
                    filtered.add(filterByAdvertised(channelGroup));
                }
                channelGroups = filtered.build();
            }

            // This is a temporary hack for testing purposes. We do not want to show the new
            // duplicate BT channels that will have a start date somewhere 5 years in the future.
            // This should be removed once we deliver the channel grouping tool
            if (application.getTitle().equals("BT TVE Prod") && !futureChannels) {
                ImmutableList.Builder filtered = ImmutableList.builder();
                for (ChannelGroup channelGroup : channelGroups) {
                    filtered.add(filterByChannelStartDate(channelGroup));
                }
                channelGroups = filtered.build();
            }


            if (!Strings.isNullOrEmpty(dttOnly)) {
                List<String> dttIds = Arrays.asList(dttOnly.split("\\s*,\\s*"));
                ImmutableList.Builder filtered = ImmutableList.builder();
                for (ChannelGroup channelGroup : channelGroups) {
                    String channelGroupId = oldFormatIdCodec.encode(BigInteger.valueOf(channelGroup.getId()));
                    if (dttIds.contains(channelGroupId)) {
                        filtered.add(filterByDtt(channelGroup));
                    } else {
                        filtered.add(channelGroup);
                    }
                    channelGroups = filtered.build();
                }
            }

            if (!Strings.isNullOrEmpty(ipOnly)) {
                List<String> ipIds = Arrays.asList(ipOnly.split("\\s*,\\s*"));
                ImmutableList.Builder filtered = ImmutableList.builder();
                for (ChannelGroup channelGroup : channelGroups) {
                    String channelGroupId = oldFormatIdCodec.encode(BigInteger.valueOf(channelGroup.getId()));
                    if (ipIds.contains(channelGroupId)) {
                        filtered.add(filterByIp(channelGroup));
                    } else {
                        filtered.add(channelGroup);
                    }
                }
                channelGroups = filtered.build();
            }

            modelAndViewFor(request, response, channelGroups, application);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    @RequestMapping(value={"/3.0/channel_groups/{id}.*", "/channel_groups/{id}.*"})
    public void listChannel(HttpServletRequest request, HttpServletResponse response, 
            @PathVariable("id") String id, 
            @RequestParam(value = CHANNEL_GENRES_KEY, required = false) String channelGenres,
            @RequestParam(value = ADVERTISED, required = false) String advertised,
            @RequestParam(value = DTT_ONLY, defaultValue = "false", required = false) boolean dttOnly,
            @RequestParam(value = IP_ONLY, defaultValue = "false", required = false) boolean ipOnly,
            @RequestParam(value = BT_FUTURE_CHANNELS, defaultValue = "true", required = false) boolean futureChannels,
            @RequestParam(value = REFRESH_CACHE, defaultValue = "false", required = false) boolean cacheRefresh,
            @RequestParam(value = ID_FORMAT, required = false, defaultValue = OWL) String idFormat
    ) throws IOException {
        try {
            Application application;
            try {
                application = application(request);
            } catch (InvalidApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }

            long numericalId;
            if(idFormat.toLowerCase().equals(DEER)) {
                numericalId = newFormatIdCodec.decode(id).longValue();
            } else {
                numericalId = oldFormatIdCodec.decode(id).longValue();
            }

            if(cacheRefresh){
                channelGroupResolver.invalidateCache(numericalId);
            }

            Optional<ChannelGroup> possibleChannelGroup =
                    channelGroupResolver.channelGroupFor(numericalId);

            if (!possibleChannelGroup.isPresent()) {
                errorViewFor(request, response, NOT_FOUND);
                return;
            }

            if (!application.getConfiguration()
                    .isReadEnabled(possibleChannelGroup.get().getPublisher())) {
                outputter.writeError(
                        request,
                        response,
                        FORBIDDEN.withMessage("ChannelGroup " + id + " not available")
                );
                return;
            }
            
            Optional<Set<Annotation>> annotations = annotationExtractor.extract(request);
            if (annotations.isPresent() && !validAnnotations(annotations.get())) {
                errorViewFor(request, response, BAD_ANNOTATION);
                return;
            }
            ChannelGroup toReturn;
            if (!Strings.isNullOrEmpty(channelGenres)) {
                Set<String> genres = ImmutableSet.copyOf(SPLIT_ON_COMMA.split(channelGenres));
                toReturn = filterByChannelGenres(possibleChannelGroup.get(), genres);
            } else {
                toReturn = possibleChannelGroup.get();
            }

            if (!Strings.isNullOrEmpty(advertised)) {
                toReturn = filterByAdvertised(toReturn);
            }

            // This is a temporary hack for testing purposes. We do not want to show the new
            // duplicate BT channels that will have a start date somewhere 5 years in the future.
            // This should be removed once we deliver the channel grouping tool
            if (!futureChannels) {
                if (application.getTitle().equals("BT TVE Prod")) {
                    toReturn = filterByChannelStartDate(toReturn);
                }
            }

            if (dttOnly) {
                toReturn = filterByDtt(toReturn);
            }

            if (ipOnly) {
                toReturn = filterByIp(toReturn);
            }

            modelAndViewFor(request, response, ImmutableList.of(toReturn), application);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    @RequestMapping(value = { "/3.0/channel_groups.*" }, method = RequestMethod.POST)
    public WriteResponseWithOldIds createChannelGroup(
            HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam(value = ID_FORMAT, required = false, defaultValue = OWL) String idFormat
    ) {
        return createOrUpdateChannelGroup(request, response, idFormat);
    }

    @RequestMapping(value = { "/3.0/channel_groups.*" }, method = RequestMethod.PUT)
    public WriteResponseWithOldIds updateChannelGroup(
            HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam(value = ID_FORMAT, required = false, defaultValue = OWL) String idFormat
    ) {
        return createOrUpdateChannelGroup(request, response, idFormat);
    }

    private WriteResponseWithOldIds createOrUpdateChannelGroup(
            HttpServletRequest request,
            HttpServletResponse response,
            String idFormat
    ) {
        java.util.Optional<Application> possibleApplication;
        try {
            possibleApplication = applicationFetcher.applicationFor(request);
        } catch (InvalidApiKeyException ex) {
            return error(request, response, AtlasErrorSummary.forException(ex));
        }
        if (!possibleApplication.isPresent()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(new UnauthorizedException())
            );
        }

        ChannelGroup complexChannelGroup;
        org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup;

        try {
            simpleChannelGroup = deserialize(new InputStreamReader(request.getInputStream()));
            if("region".equals(simpleChannelGroup.getType())) {
                complexChannelGroup = new Region();
            }
            else {
                complexChannelGroup = new Platform();
            }
            if(idFormat.toLowerCase().equals(DEER)) {
                convertFromDeerToOwlIds(simpleChannelGroup);
            }
            complexChannelGroup = complexify(simpleChannelGroup);

        } catch (UnrecognizedPropertyException |
                JsonParseException |
                ConstraintViolationException e) {
            return error(request, response, AtlasErrorSummary.forException(e));

        } catch (IOException e) {
            log.error("Error reading input for request {}", request.getRequestURL(), e);
            return error(request, response, AtlasErrorSummary.forException(e));

        } catch (Exception e) {
            log.error("Error reading input for request {}", request.getRequestURL(), e);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(request, response, errorSummary);
        }

        if (!possibleApplication.get()
                .getConfiguration()
                .isWriteEnabled(complexChannelGroup.getPublisher())) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(new ForbiddenException())
            );
        }

        Optional<ChannelGroup> channelGroup = channelGroupWriteExecutor.createOrUpdateChannelGroup(
                request,
                complexChannelGroup,
                simpleChannelGroup.getChannels(),
                channelResolver
        );

        if (!channelGroup.isPresent()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(new BadRequestException("Error while creating/updating platform"))
            );
        }

        response.setStatus(HttpServletResponse.SC_OK);

        return new WriteResponseWithOldIds(
                oldFormatIdCodec.encode(BigInteger.valueOf(channelGroup.get().getId())),
                newFormatIdCodec.encode(BigInteger.valueOf(channelGroup.get().getId())));
    }

    private void convertFromDeerToOwlIds(
            org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup
    ) {
        simpleChannelGroup.getChannels().forEach(channelNumbering -> {
            String deerId = channelNumbering.getChannel().getId();
            String owlId = oldFormatIdCodec.encode(newFormatIdCodec.decode(deerId));
            channelNumbering.getChannel().setId(owlId);
        });

        if(!Strings.isNullOrEmpty(simpleChannelGroup.getId())){
            String deerId = simpleChannelGroup.getId();
            String owlId = oldFormatIdCodec.encode(newFormatIdCodec.decode(deerId));
            simpleChannelGroup.setId(owlId);
        }
    }

    @RequestMapping(value = { "/3.0/channel_groups/{id}.*" }, method = RequestMethod.DELETE)
    public WriteResponseWithOldIds deleteChannelGroup(
            @PathVariable("id") String id,
            HttpServletRequest request,
            HttpServletResponse response,
            @RequestParam(value = ID_FORMAT, required = false, defaultValue = OWL) String idFormat
    ) throws IOException {

        if (Strings.isNullOrEmpty(id)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            errorViewFor(
                    request,
                    response,
                    AtlasErrorSummary.forException(new IllegalArgumentException(
                            "You must specify a platform ID for this action."
                    ))
            );
        }

        java.util.Optional<Application> possibleApplication;
        try {
            possibleApplication = applicationFetcher.applicationFor(request);
        } catch (InvalidApiKeyException ex) {
            return error(request, response, AtlasErrorSummary.forException(ex));
        }
        if (!possibleApplication.isPresent()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return error(request, response, AtlasErrorSummary.forException(new UnauthorizedException()));
        }

        long channelGroupId = idFormat.toLowerCase().equals(DEER)
                ? newFormatIdCodec.decode(id).longValue()
                : oldFormatIdCodec.decode(id).longValue();

        com.google.common.base.Optional<ChannelGroup> possibleChannelGroup = channelGroupResolver.channelGroupFor(
                channelGroupId
        );
        if (!possibleChannelGroup.isPresent()) {
            return error(request, response, AtlasErrorSummary.forException(new NullPointerException(
                    String.format("No such platform exists with ID %s", id)
            )));
        }
        if (!possibleApplication.get()
                .getConfiguration()
                .isWriteEnabled(possibleChannelGroup.get().getPublisher())) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return error(request, response, AtlasErrorSummary.forException(new ForbiddenException()));
        }

        java.util.Optional<AtlasErrorSummary> errorSummary = channelGroupWriteExecutor.deletePlatform(
                request,
                response,
                channelGroupId,
                channelResolver
        );

        if (errorSummary.isPresent()) {
            return error(request, response, errorSummary.get());
        }

        response.setStatus(HttpServletResponse.SC_OK);

        return null;
    }

    private WriteResponseWithOldIds error(
            HttpServletRequest request,
            HttpServletResponse response,
            AtlasErrorSummary summary
    ) {
        try {
            outputter.writeError(request, response, summary);
        } catch (IOException e) {
            log.error("Error executing request {}", request.getRequestURL(), e);
        }
        return null;
    }

    private ChannelGroup complexify(
            org.atlasapi.media.entity.simple.ChannelGroup simpleChannelGroup
    ) {
        return channelGroupTransformer.transform(simpleChannelGroup);
    }

    private org.atlasapi.media.entity.simple.ChannelGroup deserialize(Reader input)
            throws IOException, ReadException {
        return modelReader.read(
                new BufferedReader(input),
                org.atlasapi.media.entity.simple.ChannelGroup.class,
                true
        );
    }
    
    private ChannelGroup filterByChannelGenres(ChannelGroup channelGroup, final Set<String> genres) {
        Iterable<ChannelNumbering> filtered = channelGroup.getChannelNumberings()
                .stream()
                .filter(input -> {
                    Channel channel = Iterables.getOnlyElement(channelResolver.forIds(
                            ImmutableSet.of(input.getChannel())
                    ));
                    return hasMatchingGenre(channel, genres);
                })
                .collect(Collectors.toList());
        ChannelGroup filteredGroup = channelGroup.copy();
        filteredGroup.setChannelNumberings(filtered);
        return filteredGroup;
    }

    private ChannelGroup filterByDtt(ChannelGroup channelGroup) {
        List<ChannelNumbering> channelNumberings = channelGroup.getChannelNumberings()
                .stream()
                .filter(channelNumbering -> !Strings.isNullOrEmpty(channelNumbering.getChannelNumber()))
                .filter(channelNumbering -> Integer.parseInt(channelNumbering.getChannelNumber()) <= 300)
                .collect(Collectors.toList());

        ChannelGroup filteredGroup = channelGroup.copy();
        filteredGroup.setChannelNumberings(channelNumberings);
        return filteredGroup;
    }

    private ChannelGroup filterByIp(ChannelGroup channelGroup) {
        List<ChannelNumbering> channelNumberings = channelGroup.getChannelNumberings()
                .stream()
                .filter(channelNumbering -> !Strings.isNullOrEmpty(channelNumbering.getChannelNumber()))
                .filter(channelNumbering -> Integer.parseInt(channelNumbering.getChannelNumber()) > 300)
                .collect(Collectors.toList());

        ChannelGroup filteredGroup = channelGroup.copy();
        filteredGroup.setChannelNumberings(channelNumberings);
        return filteredGroup;
    }

    private ChannelGroup filterByAdvertised(ChannelGroup channelGroup) {
        Iterable<ChannelNumbering> filtered = channelGroup.getChannelNumberings()
                .stream()
                .filter(input -> {
                    Channel channel = Iterables.getOnlyElement(channelResolver.forIds(
                            ImmutableSet.of(input.getChannel())
                    ));
                    return isAdvertised(channel);
                })
                .collect(Collectors.toList());
        ChannelGroup filteredGroup = channelGroup.copy();
        filteredGroup.setChannelNumberings(filtered);
        return filteredGroup;
    }

    private ChannelGroup filterByChannelStartDate(ChannelGroup channelGroup) {
        Set<ChannelNumbering> filtered = channelGroup.getChannelNumberings()
                .stream()
                .filter(channelNumbering -> channelNumbering.getStartDate().isBefore(LocalDate.now()))
                .collect(Collectors.toSet());
        ChannelGroup filteredGroup = channelGroup.copy();
        filteredGroup.setChannelNumberings(filtered);
        return filteredGroup;
    }

    private boolean hasMatchingGenre(Channel channel, Set<String> genres) {
        return !Sets.intersection(channel.getGenres(), genres).isEmpty();
    }

    private boolean isAdvertised(Channel channel) {
        return channel.getAdvertiseFrom() == null || !channel.getAdvertiseFrom().isAfterNow();
    }

    private boolean validAnnotations(Set<Annotation> annotations) {
        return validAnnotations.containsAll(annotations);
    }
    
    private ChannelGroupFilter constructFilter(String platformId, String type, String advertised) {
        ChannelGroupFilterBuilder filter = ChannelGroupFilter.builder();
        
        if (!Strings.isNullOrEmpty(platformId)) {
            // resolve platform if present
            Optional<ChannelGroup> possiblePlatform = channelGroupResolver.channelGroupFor(
                    oldFormatIdCodec.decode(platformId).longValue()
            );
            if (!possiblePlatform.isPresent()) {
                throw new IllegalArgumentException("could not resolve channel group with id " + platformId);
            }
            if (!(possiblePlatform.get() instanceof Platform)) {
                throw new IllegalArgumentException("channel group with id " + platformId + " not a platform");
            }
            filter.withPlatform((Platform)possiblePlatform.get());
        }

        if (!Strings.isNullOrEmpty(type)) {
            // resolve channelGroup type
            if (type.equals("platform")) {
                filter.withType(ChannelGroupType.PLATFORM);
            } else if (type.equals("region")) {
                filter.withType(ChannelGroupType.REGION);
            } else {
                throw new IllegalArgumentException("type provided was not valid, should be either platform or region");
            }
        }

        return filter.build();
    }

    public static class Builder {

        private ApplicationFetcher applicationFetcher;
        private AdapterLog log;
        private AtlasModelWriter<Iterable<ChannelGroup>> atlasModelWriter;
        private ChannelGroupResolver channelGroupResolver;
        private ChannelGroupTransformer channelGroupTransformer;
        private ChannelGroupWriteExecutor channelGroupWriteExecutor;
        private ChannelResolver channelResolver;
        private ModelReader modelReader;

        public Builder withApplicationFetcher(ApplicationFetcher configFetcher) {
            this.applicationFetcher = configFetcher;
            return this;
        }

        public Builder withLog(AdapterLog log) {
            this.log = log;
            return this;
        }

        public Builder withAtlasModelWriter(AtlasModelWriter<Iterable<ChannelGroup>> atlasModelWriter) {
            this.atlasModelWriter = atlasModelWriter;
            return this;
        }

        public Builder withChannelGroupResolver(ChannelGroupResolver channelGroupResolver) {
            this.channelGroupResolver = channelGroupResolver;
            return this;
        }

        public Builder withChannelGroupTransformer(ChannelGroupTransformer channelGroupTransformer) {
            this.channelGroupTransformer = channelGroupTransformer;
            return this;
        }

        public Builder withChannelGroupWriteExecutor(
                ChannelGroupWriteExecutor channelGroupWriteExecutor
        ) {
            this.channelGroupWriteExecutor = channelGroupWriteExecutor;
            return this;
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        public Builder withModelReader(ModelReader modelReader) {
            this.modelReader = modelReader;
            return this;
        }

        public ChannelGroupController build() {
            return new ChannelGroupController(this);
        }
    }
}
