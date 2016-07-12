package org.atlasapi.query.v2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.core.HttpHeaders;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.QueryResult;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.query.content.ContentWriteExecutor;
import org.atlasapi.query.worker.ContentWriteMessage;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.api.client.util.Maps;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Flushables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriteController {

    public static final String ASYNC_PARAMETER = "async";

    private static final boolean MERGE = true;
    private static final boolean OVERWRITE = false;

    private static final Logger log = LoggerFactory.getLogger(ContentWriteController.class);
    private static final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final ApplicationConfigurationFetcher appConfigFetcher;
    private final ContentWriteExecutor writeExecutor;
    private final LookupBackedContentIdGenerator lookupBackedContentIdGenerator;
    private final MessageSender<ContentWriteMessage> messageSender;
    private final AtlasModelWriter<QueryResult<Identified, ? extends Identified>> outputWriter;
    private final Gson gson = new GsonBuilder().create();

    public ContentWriteController(
            ApplicationConfigurationFetcher appConfigFetcher,
            ContentWriteExecutor contentWriteExecutor,
            LookupBackedContentIdGenerator lookupBackedContentIdGenerator,
            MessageSender<ContentWriteMessage> messageSender,
            AtlasModelWriter<QueryResult<Identified, ? extends Identified>> outputWriter
    ) {
        this.appConfigFetcher = checkNotNull(appConfigFetcher);
        this.writeExecutor = checkNotNull(contentWriteExecutor);
        this.lookupBackedContentIdGenerator = checkNotNull(lookupBackedContentIdGenerator);
        this.messageSender = checkNotNull(messageSender);
        this.outputWriter = outputWriter;
    }

    @RequestMapping(value = "/3.0/content.json", method = RequestMethod.POST)
    public Void postContent(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        return deserializeAndUpdateContent(req, resp, MERGE);
    }

    @RequestMapping(value = "/3.0/content.json", method = RequestMethod.PUT)
    public Void putContent(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        return deserializeAndUpdateContent(req, resp, OVERWRITE);
    }

    private Void deserializeAndUpdateContent(HttpServletRequest req, HttpServletResponse resp,
            boolean merge) throws IOException {
        Boolean async = Boolean.valueOf(req.getParameter(ASYNC_PARAMETER));

        Maybe<ApplicationConfiguration> possibleConfig;
        try {
            possibleConfig = appConfigFetcher.configurationFor(req);
        } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
            return error(req, resp, AtlasErrorSummary.forException(ex));
        }

        if (possibleConfig.isNothing()) {

            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new UnauthorizedException(
                            "API key is unauthorised"
                    ))
            );
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
            inputContent = writeExecutor.parseInputStream(inputStream);
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

        if (!possibleConfig.requireValue().canWrite(content.getPublisher())) {

            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new ForbiddenException(
                            "API key does not have write permission"
                    ))
            );
        }

        Long contentId = lookupBackedContentIdGenerator.getId(content);

        try {
            if (async) {
                sendMessage(inputStreamBytes, contentId, merge);
            } else {
                content.setId(contentId);
                writeExecutor.writeContent(content, inputContent.getType(), merge);
            }
        } catch (IllegalArgumentException | NullPointerException e) {
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
        OutputStream out = resp.getOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(out, Charsets.UTF_8);
        try {
            String str = gson.toJson(encodeId(contentId));
            resp.setContentLength(str.getBytes().length);
            writer.write(str);
        } finally {
            Flushables.flushQuietly(out);
        }
        return null;
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

    private Void error(HttpServletRequest request, HttpServletResponse response,
            AtlasErrorSummary summary) {
        try {
            outputWriter.writeError(request, response, summary);
        } catch (IOException e) {
            logError("Error executing request", e, request);
        }
        return null;
    }

    protected class Id {

        private final String id;

        public Id(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

    }
}
