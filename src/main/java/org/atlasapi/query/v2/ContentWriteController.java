package org.atlasapi.query.v2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.util.Maps;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.social.exceptions.BadRequestException;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.common.time.Timestamp;
import org.apache.commons.io.IOUtils;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.response.WriteResponse;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.QueryResult;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.query.content.ContentWriteExecutor;
import org.atlasapi.query.content.merge.BroadcastMerger;
import org.atlasapi.query.worker.ContentWriteMessage;
import org.atlasapi.remotesite.util.OldContentDeactivator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.core.HttpHeaders;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriteController {

    public static final String ASYNC_PARAMETER = "async";
    public static final String BROADCAST_ASSERTIONS_PARAMETER = "broadcastAssertions";
    private static final String STRICT_PARAMETER = "strict";
    private static final String SOURCE_PARAMETER = "source";
    private static final String VALID_URIS_PARAMETER = "valid_uris";

    private static final String ID = "id";
    private static final String URI = "uri";
    private static final String EXPLICIT = "explicit";

    private static final boolean MERGE = true;
    private static final boolean OVERWRITE = false;

    private static final Logger log = LoggerFactory.getLogger(ContentWriteController.class);
    private static final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private static final Splitter COMMA_SPLITTER = Splitter.on(',');

    private final ApplicationFetcher applicationFetcher;
    private final ContentWriteExecutor writeExecutor;
    private final LookupBackedContentIdGenerator lookupBackedContentIdGenerator;
    private final MessageSender<ContentWriteMessage> messageSender;
    private final AtlasModelWriter<QueryResult<Identified, ? extends Identified>> outputWriter;
    private final LookupEntryStore lookupEntryStore;
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final EquivalenceBreaker equivalenceBreaker;
    private final OldContentDeactivator oldContentDeactivator;

    private ContentWriteController(
            ApplicationFetcher applicationFetcher,
            ContentWriteExecutor contentWriteExecutor,
            LookupBackedContentIdGenerator lookupBackedContentIdGenerator,
            MessageSender<ContentWriteMessage> messageSender,
            AtlasModelWriter<QueryResult<Identified, ? extends Identified>> outputWriter,
            LookupEntryStore lookupEntryStore,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            EquivalenceBreaker equivalenceBreaker,
            OldContentDeactivator oldContentDeactivator
    ) {
        this.applicationFetcher = checkNotNull(applicationFetcher);
        this.writeExecutor = checkNotNull(contentWriteExecutor);
        this.lookupBackedContentIdGenerator = checkNotNull(lookupBackedContentIdGenerator);
        this.messageSender = checkNotNull(messageSender);
        this.outputWriter = checkNotNull(outputWriter);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentResolver = checkNotNull(contentResolver);
        this.contentWriter = checkNotNull(contentWriter);
        this.equivalenceBreaker = checkNotNull(equivalenceBreaker);
        this.oldContentDeactivator = checkNotNull(oldContentDeactivator);
    }

    public static ContentWriteController create(
            ApplicationFetcher applicationFetcher,
            ContentWriteExecutor contentWriteExecutor,
            LookupBackedContentIdGenerator lookupBackedContentIdGenerator,
            MessageSender<ContentWriteMessage> messageSender,
            AtlasModelWriter<QueryResult<Identified, ? extends Identified>> outputWriter,
            LookupEntryStore lookupEntryStore,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            EquivalenceBreaker equivalenceBreaker,
            OldContentDeactivator oldContentDeactivator
    ) {
        return new ContentWriteController(
                applicationFetcher,
                contentWriteExecutor,
                lookupBackedContentIdGenerator,
                messageSender,
                outputWriter,
                lookupEntryStore,
                contentResolver,
                contentWriter,
                equivalenceBreaker,
                oldContentDeactivator
        );
    }

    @Nullable
    public WriteResponse postContent(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateContent(req, resp, MERGE);
    }

    @Nullable
    public WriteResponse putContent(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateContent(req, resp, OVERWRITE);
    }

    @Nullable
    public WriteResponse unpublishContent(HttpServletRequest req, HttpServletResponse resp) {

        PossibleApplication possibleApplication = validateApplicationConfiguration(
                req,
                resp
        );
        if (possibleApplication.getErrorSummary().isPresent()) {
            return error(req, resp, possibleApplication.getErrorSummary().get());
        }

        if (!Strings.isNullOrEmpty(req.getParameter(SOURCE_PARAMETER))
                && !req.getParameter(VALID_URIS_PARAMETER).isEmpty()) {
            return unpublishOldContent(req, resp, possibleApplication.getApplication());
        }

        return setPublishStatus(req, resp, false, possibleApplication.getApplication());
    }

    @Nullable
    private WriteResponse unpublishOldContent(
            HttpServletRequest request,
            HttpServletResponse response,
            Optional<Application> application) {


        String source = request.getParameter(SOURCE_PARAMETER);
        ImmutableList<String> validUris = ImmutableList.copyOf(
                request.getParameter(VALID_URIS_PARAMETER)
                        .split(",")
        );

        Publisher publisher = Publisher.fromKey(source).requireValue();

        Optional<AtlasErrorSummary> apiKeyErrorSummary = validateApiKey(
                request,
                response,
                publisher,
                application
        );
        if (apiKeyErrorSummary.isPresent()) {
            return error(request, response, apiKeyErrorSummary.get());
        }

        if (oldContentDeactivator.deactivateOldContent(publisher, validUris, 50)) {
            response.setStatus(HttpServletResponse.SC_OK);
        }

        return null;
    }

    /**
     * This was added as a way to update the explicit equivalence on an existing piece of content without having to
     * hit the regular content PUT endpoint since we may not care to change anything about the actual content itself.
     * N.B. this updates the content table since it contains a copy of the explicit set and is used for merging
     * explicit equivalences in the regular content POST endpoint.
     *
     * In particular this allows an easy way to remove all explicit equivalences on a piece of content since the other
     * methods do not support updating the explicit equivalences with an empty set.
     */
    @Nullable
    public WriteResponse updateExplicitEquivalence(HttpServletRequest req, HttpServletResponse resp) {
        PossibleApplication possibleApplication = validateApplicationConfiguration(req, resp);
        if (possibleApplication.getErrorSummary().isPresent()) {
            return error(req, resp, possibleApplication.getErrorSummary().get());
        }
        String uri = req.getParameter(URI);
        if(Strings.isNullOrEmpty(uri) || req.getParameter(EXPLICIT) == null) {
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new BadRequestException(
                            String.format("'%s' / '%s' not specified", URI, EXPLICIT)
                    ))
            );
        }

        String sources = req.getParameter(SOURCE_PARAMETER);
        ImmutableSet<Publisher> publishers = Strings.isNullOrEmpty(sources)
                ? Publisher.all()
                : COMMA_SPLITTER.splitToList(sources).stream()
                        .map(Publisher::fromKey)
                        .filter(Maybe::hasValue)
                        .map(Maybe::requireValue)
                        .collect(MoreCollectors.toImmutableSet());

        ResolvedContent resolvedContent =
                contentResolver.findByCanonicalUris(Lists.newArrayList(uri));

        @SuppressWarnings("deprecation")
        Maybe<Identified> identified;

        if (!resolvedContent.isEmpty()) {
            identified = resolvedContent.getFirstValue();
        } else {
            return error(req, resp, AtlasErrorSummary.forException(
                    new NoSuchElementException("could not resolve content for URI")
            ));
        }

        // If we didn't find one, return an error
        if (!identified.hasValue()) {
            return error(req, resp, AtlasErrorSummary.forException(
                    new NoSuchElementException("Unable to resolve content")));
        }

        Content content = (Content) identified.requireValue();

        // Check if the api key has write permissions for this publisher
        Optional<AtlasErrorSummary> apiKeyErrorSummary = validateApiKey(
                req,
                resp,
                content.getPublisher(),
                possibleApplication.getApplication()
        );
        if (apiKeyErrorSummary.isPresent()) {
            return error(req, resp, apiKeyErrorSummary.get());
        }

        Iterable<LookupEntry> explicitEquivLookupEntries = lookupEntryStore.entriesForCanonicalUris(
                COMMA_SPLITTER.splitToList(req.getParameter(EXPLICIT))
        );
        ImmutableSet<LookupRef> explicitEquivLookupRefs = MoreStreams.stream(explicitEquivLookupEntries)
                .map(LookupEntry::lookupRef)
                .collect(MoreCollectors.toImmutableSet());

        writeExecutor.updateExplicitEquivalence(content, publishers, explicitEquivLookupRefs);

        resp.setStatus(HttpServletResponse.SC_OK);
        return new WriteResponse(encodeId(content.getId()));
    }

    private PossibleApplication validateApplicationConfiguration(
            HttpServletRequest request,
            HttpServletResponse response
    ) {
        PossibleApplication possibleApplication = new PossibleApplication();
        try {
            possibleApplication.setApplication(applicationFetcher.applicationFor(request));
        } catch (InvalidApiKeyException ex) {
            possibleApplication.setErrorSummary(Optional.of(AtlasErrorSummary.forException(ex)));
        }

        if (!possibleApplication.getApplication().isPresent()) {
            possibleApplication.setErrorSummary(Optional.of(
                    AtlasErrorSummary.forException(new UnauthorizedException(
                            "API key is unauthorised"
                    ))
            ));
        }

        return possibleApplication;
    }

    private Optional<AtlasErrorSummary> validateApiKey(
            HttpServletRequest request,
            HttpServletResponse response,
            Publisher publisher,
            Optional<Application> possibleApplication) {
        if (!possibleApplication.isPresent() || !possibleApplication.get().getConfiguration().isWriteEnabled(publisher)) {
            return Optional.of(
                    AtlasErrorSummary.forException(new ForbiddenException(
                            "API key does not have write permission"
                    ))
            );
        }
        return Optional.empty();
    }

    @Nullable
    private WriteResponse setPublishStatus(
            HttpServletRequest req,
            HttpServletResponse resp,
            boolean publishStatus,
            Optional<Application> possibleApplication) {

        // get ID / URI, if ID, lookup URI from it
        LookupEntry lookupEntry;
        if (req.getParameter(ID) != null) {
            Long contentId = SubstitutionTableNumberCodec
                    .lowerCaseOnly()
                    .decode(req.getParameter(ID))
                    .longValue();
            Iterator<LookupEntry> entryStoreIterator = lookupEntryStore
                    .entriesForIds(Lists.newArrayList(contentId))
                    .iterator();
            if (entryStoreIterator.hasNext()) {
                lookupEntry = entryStoreIterator.next();
            } else {
                return error(req, resp, AtlasErrorSummary.forException(
                        new NoSuchElementException("Content not found for id"))
                );
            }
        } else if (req.getParameter(URI) != null) {
            Iterator<LookupEntry> entryStoreIterator = lookupEntryStore
                    .entriesForCanonicalUris(Lists.newArrayList(req.getParameter(URI)))
                    .iterator();
            if (entryStoreIterator.hasNext()) {
                lookupEntry = entryStoreIterator.next();
            } else {
                return error(req, resp, AtlasErrorSummary.forException(
                        new NoSuchElementException("Content not found for uri"))
                );
            }
        } else {
            return error(req, resp, AtlasErrorSummary.forException(
                    new BadRequestException("id / uri parameter not specified"))
            );
        }

        // find some content from URI
        @SuppressWarnings("deprecation")
        Maybe<Identified> identified;

        ResolvedContent resolvedContent =
                contentResolver.findByCanonicalUris(Lists.newArrayList(lookupEntry.uri()));

        if (!resolvedContent.isEmpty()) {
            identified = resolvedContent.getFirstValue();
        } else {
            return error(req, resp, AtlasErrorSummary.forException(
                    new NoSuchElementException("could not resolve content for URI")
            ));
        }

        // If we didn't find one, return an error
        if (!identified.hasValue()) {
            return error(req, resp, AtlasErrorSummary.forException(
                    new NoSuchElementException("Unable to resolve content")));
        }

        Described described = (Described) identified.requireValue();

        // Check if the api key has write permissions for this publisher
        Optional<AtlasErrorSummary> apiKeyErrorSummary = validateApiKey(
                req,
                resp,
                described.getPublisher(),
                possibleApplication
        );
        if (apiKeyErrorSummary.isPresent()) {
            return error(req, resp, apiKeyErrorSummary.get());
        }

        // remove from equivset if un-publishing
        if(!publishStatus){
            removeItemFromEquivSet(described, lookupEntry);
        }

        // set publisher status
        described.setActivelyPublished(publishStatus);

        // write back to DB
        if (described instanceof Item) {
            contentWriter.createOrUpdate((Item) described);
        }
        if (described instanceof Container) {
            contentWriter.createOrUpdate((Container) described);
        }

        // return all good
        resp.setStatus(HttpServletResponse.SC_OK);
        return null;
    }

    /**
     * This will take an item, and remove all direct equivalences from it
     */
    private void removeItemFromEquivSet(Described described, LookupEntry lookupEntry){

        ImmutableSet<String> equivsToRemove = lookupEntry.directEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());

        equivalenceBreaker.removeFromSet(described, lookupEntry, equivsToRemove);
    }


    @Nullable
    private WriteResponse deserializeAndUpdateContent(
            HttpServletRequest req,
            HttpServletResponse resp,
            boolean merge
    ) {
        Boolean async = Boolean.valueOf(req.getParameter(ASYNC_PARAMETER));
        Boolean strict = Boolean.valueOf(req.getParameter(STRICT_PARAMETER));

        String broadcastAssertionsParameter = req.getParameter(BROADCAST_ASSERTIONS_PARAMETER);
        BroadcastMerger broadcastMerger = BroadcastMerger.parse(
                broadcastAssertionsParameter
        );

        if (async && !Strings.isNullOrEmpty(broadcastAssertionsParameter)) {
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new IllegalArgumentException(
                            "The '" + ASYNC_PARAMETER + "' and '" + BROADCAST_ASSERTIONS_PARAMETER
                                    + "' request parameters are mutually exclusive"
                    ))
            );
        }

        if (!merge && !Strings.isNullOrEmpty(broadcastAssertionsParameter)) {
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new IllegalArgumentException(
                            "'" + BROADCAST_ASSERTIONS_PARAMETER
                                    + "' request parameters are not supported with PUT"
                    ))
            );
        }

        PossibleApplication possibleApplication = validateApplicationConfiguration(req, resp);
        if (possibleApplication.getErrorSummary().isPresent()) {
            return error(req, resp, possibleApplication.getErrorSummary().get());
        }

        byte[] inputStreamBytes;
        ContentWriteExecutor.InputContent inputContent;
        try {
            // We are wrapping the stream in a ByteArrayInputStream to allow us to read the
            // stream multiple times. This so we can deserialise here to do the validation
            // and then pass the same stream to the message sender if the async option is enabled
            // without having to reserialise it
            inputStreamBytes = IOUtils.toByteArray(req.getInputStream());
            InputStream inputStream = new ByteArrayInputStream(inputStreamBytes);
            inputContent = writeExecutor.parseInputStream(inputStream, strict);
        } catch (UnrecognizedPropertyException |
                JsonParseException |
                ConstraintViolationException e) {

            return error(req, resp, AtlasErrorSummary.forException(e));

        } catch (IOException e) {
            logError("Error reading input for request", e, req);
            return error(req, resp, AtlasErrorSummary.forException(e));

        } catch (Exception e) {
            logError("Error reading input for request", e, req);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withMessage("Error reading input for the request")
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);
        }

        Content content = inputContent.getContent();

        Optional<AtlasErrorSummary> apiKeyErrorSummary = validateApiKey(
                req,
                resp,
                content.getPublisher(),
                possibleApplication.getApplication()
        );
        if (apiKeyErrorSummary.isPresent()) {
            return error(req, resp, apiKeyErrorSummary.get());
        }

        Long contentId = lookupBackedContentIdGenerator.getId(content);

        try {
            if (async) {
                sendMessage(inputStreamBytes, contentId, merge);
            } else {

                content.setId(contentId);
                long startTime = System.nanoTime();
                writeExecutor.writeContent(content, inputContent.getType(), merge,
                        broadcastMerger
                );
                long endTime = System.nanoTime();

                long duration = (endTime - startTime)/1000000;
                if(duration > 1000){
                    log.info("TIMER SLOW CONTROLLER UPDATE {}. {} {}",duration,content.getId(), Thread.currentThread().getName());
                }
            }
        } catch (IllegalArgumentException | NullPointerException e) {
            logError("Error executing request", e, req);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e).withStatusCode(
                    HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);
        } catch (Exception e) {
            logError("Error executing request", e, req);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withMessage("Error reading input for the request")
                    .withStatusCode(HttpStatusCode.SERVER_ERROR);
            return error(req, resp, errorSummary);
        }

        setLocationHeader(resp, contentId);

        HttpStatus responseStatus = async ? HttpStatus.ACCEPTED : HttpStatus.OK;
        resp.setStatus(responseStatus.value());
        return new WriteResponse(encodeId(contentId));
    }

    private void sendMessage(byte[] inputStreamBytes, Long contentId, boolean merge)
            throws com.metabroadcast.common.queue.MessagingException {
        ContentWriteMessage contentWriteMessage = new ContentWriteMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(DateTime.now(DateTimeZone.UTC)),
                inputStreamBytes,
                contentId,
                merge
        );
        messageSender.sendMessage(contentWriteMessage, String.valueOf(contentId).getBytes());
    }

    private void setLocationHeader(HttpServletResponse resp, Long contentId) {
        String hostName = Configurer.get("local.host.name").get();
        resp.setHeader(
                HttpHeaders.LOCATION,
                hostName
                        + "/3.0/content.json?id="
                        + encodeId(contentId)
        );
    }

    private String encodeId(Long contentId) {
        return codec.encode(BigInteger.valueOf(contentId));
    }

    private void logError(String errorMessage, Exception e, HttpServletRequest req) {
        StringBuilder errorBuilder = new StringBuilder();

        errorBuilder.append(errorMessage)
                .append(" ")
                .append(req.getRequestURL());

        Map<String, String> parameters = Maps.newHashMap();
        for (Map.Entry<String, String[]> parameter : req.getParameterMap().entrySet()) {
            parameters.put(parameter.getKey(), Joiner.on(",").join(parameter.getValue()));
        }

        if (!parameters.isEmpty()) {
            String parameterString = Joiner.on("&")
                    .withKeyValueSeparator("=")
                    .join(parameters);

            errorBuilder.append("?")
                    .append(parameterString);
        }

        log.error(errorBuilder.toString(), e);
    }

    private WriteResponse error(
            HttpServletRequest request,
            HttpServletResponse response,
            AtlasErrorSummary summary
    ) {
        try {
            outputWriter.writeError(request, response, summary);
        } catch (IOException e) {
            logError("Error executing request", e, request);
        }
        return null;
    }

    private class PossibleApplication{
        private Optional<AtlasErrorSummary> errorSummary = Optional.empty();
        private Optional<Application> application = Optional.empty();


        public Optional<AtlasErrorSummary> getErrorSummary() {
            return errorSummary;
        }

        public void setErrorSummary(Optional<AtlasErrorSummary> errorSummary) {
            this.errorSummary = errorSummary;
        }

        public Optional<Application> getApplication() {
            return application;
        }

        public void setApplication(
                Optional<Application> application) {
            this.application = application;
        }
    }

}