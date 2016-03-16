package org.atlasapi.query.v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ModelTransformer;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.query.content.ContentWriteExecutor;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentWriteController {

    //TODO: replace with proper merge strategies.
    private static final boolean MERGE = true;
    private static final boolean OVERWRITE = false;

    private static final Logger log = LoggerFactory.getLogger(ContentWriteController.class);
    private static final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final ApplicationConfigurationFetcher appConfigFetcher;
    private final ModelReader reader;
    private final ModelTransformer<Description, Content> transformer;
    private final ContentWriteExecutor writeExecutor;
    private final LookupBackedContentIdGenerator lookupBackedContentIdGenerator;

    public ContentWriteController(
            ApplicationConfigurationFetcher appConfigFetcher,
            ModelReader reader,
            ModelTransformer<Description, Content> transformer,
            ContentWriteExecutor contentWriteExecutor,
            LookupBackedContentIdGenerator lookupBackedContentIdGenerator
    ) {
        this.appConfigFetcher = checkNotNull(appConfigFetcher);
        this.reader = checkNotNull(reader);
        this.transformer = checkNotNull(transformer);
        this.writeExecutor = checkNotNull(contentWriteExecutor);
        this.lookupBackedContentIdGenerator = checkNotNull(lookupBackedContentIdGenerator);
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
        Maybe<ApplicationConfiguration> possibleConfig;
        try {
            possibleConfig = appConfigFetcher.configurationFor(req);
        } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }

        if (possibleConfig.isNothing()) {
            return error(resp, HttpStatus.UNAUTHORIZED.value());
        }

        Content content;
        Description description;
        try {
            description = deserialize(new InputStreamReader(req.getInputStream()));
            content = complexify(description);
        } catch (IOException ioe) {
            log.error("Error reading input for request " + req.getRequestURL(), ioe);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }

        if (!possibleConfig.requireValue().canWrite(content.getPublisher())) {
            return error(resp, HttpStatusCode.FORBIDDEN.code());
        }

        if (Strings.isNullOrEmpty(content.getCanonicalUri())) {
            return error(resp, HttpStatusCode.BAD_REQUEST.code());
        }

        Long contentId = lookupBackedContentIdGenerator.getId(content);
        content.setId(contentId);

        try {
            writeExecutor.writeContent(content, description.getType(), merge);
        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            return error(resp, HttpStatusCode.SERVER_ERROR.code());
        }

        String hostName = Configurer.get("local.host.name").get();
        resp.setHeader(
                HttpHeaders.LOCATION,
                hostName
                        + "/3.0/content.json?id="
                        + codec.encode(BigInteger.valueOf(contentId))
        );
        resp.setStatus(HttpStatusCode.OK.code());
        resp.setContentLength(0);
        return null;
    }

    private Description deserialize(Reader input) throws IOException, ReadException {
        return reader.read(new BufferedReader(input), Description.class);
    }

    private Content complexify(Description inputContent) {
        return transformer.transform(inputContent);
    }

    private Void error(HttpServletResponse response, int code) {
        response.setStatus(code);
        response.setContentLength(0);
        return null;
    }
}
