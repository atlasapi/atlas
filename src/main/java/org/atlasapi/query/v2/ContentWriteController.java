package org.atlasapi.query.v2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Content;
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

import com.google.common.base.Strings;
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

    public ContentWriteController(
            ApplicationConfigurationFetcher appConfigFetcher,
            ContentWriteExecutor contentWriteExecutor,
            LookupBackedContentIdGenerator lookupBackedContentIdGenerator,
            MessageSender<ContentWriteMessage> messageSender
    ) {
        this.appConfigFetcher = checkNotNull(appConfigFetcher);
        this.writeExecutor = checkNotNull(contentWriteExecutor);
        this.lookupBackedContentIdGenerator = checkNotNull(lookupBackedContentIdGenerator);
        this.messageSender = checkNotNull(messageSender);
    }

    @RequestMapping(value = "/3.0/content.json", method = RequestMethod.POST)
    public Void postContent(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateContent(req, resp, MERGE);
    }

    @RequestMapping(value = "/3.0/content.json", method = RequestMethod.PUT)
    public Void putContent(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateContent(req, resp, OVERWRITE);
    }

    private Void deserializeAndUpdateContent(HttpServletRequest req, HttpServletResponse resp,
            boolean merge) {
        Boolean async = Boolean.valueOf(req.getParameter(ASYNC_PARAMETER));

        Maybe<ApplicationConfiguration> possibleConfig;
        try {
            possibleConfig = appConfigFetcher.configurationFor(req);
        } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }

        if (possibleConfig.isNothing()) {
            return error(resp, HttpStatus.UNAUTHORIZED.value());
        }

        byte[] inputStreamBytes;
        ContentWriteExecutor.InputContent inputContent;
        try {
            inputStreamBytes = IOUtils.toByteArray(req.getInputStream());
            InputStream inputStream = new ByteArrayInputStream(inputStreamBytes);
            inputContent = writeExecutor.parseInputStream(inputStream);
        } catch (IOException e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }

        Content content = inputContent.getContent();

        if (!possibleConfig.requireValue().canWrite(content.getPublisher())) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }

        if (Strings.isNullOrEmpty(content.getCanonicalUri())) {
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }

        Long contentId = lookupBackedContentIdGenerator.getId(content);

        try {
            if (async) {
                sendMessage(inputStreamBytes, contentId, merge);
            } else {
                content.setId(contentId);
                writeExecutor.writeContent(content, inputContent.getType(), merge);
            }
        } catch (Exception e) {
            log.error("Error executing request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        }

        setLocationHeader(resp, contentId);

        HttpStatus responseStatus = async ? HttpStatus.ACCEPTED : HttpStatus.OK;
        resp.setStatus(responseStatus.value());
        resp.setContentLength(0);
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
                        + codec.encode(BigInteger.valueOf(contentId))
        );
    }

    private Void error(HttpServletResponse response, int code) {
        response.setStatus(code);
        response.setContentLength(0);
        return null;
    }
}
