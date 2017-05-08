package org.atlasapi.query.v2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.media.MimeType;
import joptsimple.internal.Strings;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelWriteController {

    private static final String STRICT = "strict";
    private final Logger log = LoggerFactory.getLogger(ChannelWriteController.class);
    private final ApplicationFetcher appConfigFetcher;
    private final ChannelStore store;
    private final ModelReader reader;
    private final ChannelModelTransformer channelTransformer;
    private final AtlasModelWriter<Iterable<Channel>> outputter;


    private ChannelWriteController(
            ApplicationFetcher appConfigFetcher,
            ChannelStore store,
            ModelReader reader,
            ChannelModelTransformer channelTransformer,
            AtlasModelWriter<Iterable<Channel>> outputter
    ) {
        this.appConfigFetcher = checkNotNull(appConfigFetcher);
        this.store = checkNotNull(store);
        this.reader = checkNotNull(reader);
        this.channelTransformer = checkNotNull(channelTransformer);
        this.outputter = checkNotNull(outputter);
    }

    public static ChannelWriteController create(
            ApplicationFetcher appConfigFetcher,
            ChannelStore store,
            ModelReader reader,
            ChannelModelTransformer channelTransformer,
            AtlasModelWriter<Iterable<Channel>> outputter
    ) {
        return new ChannelWriteController(
                appConfigFetcher,
                store,
                reader,
                channelTransformer,
                outputter
        );
    }

    @RequestMapping(value = "/3.0/channels", method = RequestMethod.POST)
    public Void postChannel(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateChannel(req, resp);
    }

    public Void updateChannelImage(
            HttpServletRequest request,
            HttpServletResponse response,
            NumberToShortStringCodec codec
    ) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ImageDetails incomingJson = mapper.readValue(request.getInputStream(), ImageDetails.class);

        if (Strings.isNullOrEmpty(incomingJson.getChannelId())
                || Strings.isNullOrEmpty(incomingJson.getImageTheme())
                || Strings.isNullOrEmpty(incomingJson.getImageHeight())
                || Strings.isNullOrEmpty(incomingJson.getImageWidth())) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(
                            new IllegalArgumentException(
                                    "You must specify a channel ID, theme, height and width."
                            )
                    )
            );
        }

        Optional<Application> possibleApplication;
        try {
            possibleApplication = appConfigFetcher.applicationFor(request);
        } catch (InvalidApiKeyException ex) {
            return error(request, response, AtlasErrorSummary.forException(ex));
        }

        if (!possibleApplication.isPresent()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(
                            new UnauthorizedException(
                                    "API key is unauthorised"
                            )
                    )
            );
        }

        Maybe<Channel> potentialChannel = store.fromId(codec.decode(incomingJson.getChannelId()).longValue());

        if (potentialChannel.hasValue()) {
            Channel existingChannel = potentialChannel.requireValue();

            if (Strings.isNullOrEmpty(incomingJson.getImageUri())) {
                existingChannel.deleteImageByTheme(incomingJson.getImageTheme());
            } else {
                createOrUpdateImage(
                        existingChannel,
                        incomingJson.getImageUri(),
                        incomingJson.getImageTheme(),
                        incomingJson.getImageHeight(),
                        incomingJson.getImageWidth()
                );
            }

            try {
                store.createOrUpdate(existingChannel);
            } catch (Exception e) {
                log.error("Error while creating/updating channel for request {}", request.getRequestURL(), e);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

                        return error(request, response, AtlasErrorSummary.forException(e));
                    }
                } else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    return error(
                            request,
                            response,
                            AtlasErrorSummary.forException(new NullPointerException(
                                    String.format("No channel has been found for ID %s.", incomingJson.getChannelId())
                            ))
                    );
                }

        response.setStatus(HttpServletResponse.SC_OK);
        return null;
    }

    private void createOrUpdateImage(
            Channel existingChannel,
            String imageUri,
            String imageTheme,
            String imageHeight,
            String imageWidth
    ) {
        Optional<Image> potentialImage = checkForExistingImage(existingChannel.getImages(), imageTheme);

        if (potentialImage.isPresent()) {

            Image existingImage = potentialImage.get();

            existingImage.setCanonicalUri(imageUri);
            setImageDetails(imageTheme, imageHeight, imageWidth, existingImage);

        } else {
            Image newImage = new Image(imageUri);

            setImageDetails(imageTheme, imageHeight, imageWidth, newImage);

            existingChannel.addImage(newImage);
        }
    }

    private void setImageDetails(String imageTheme, String imageHeight, String imageWidth, Image existingImage) {
        existingImage.setMimeType(MimeType.IMAGE_PNG);
        existingImage.setType(ImageType.LOGO);
        existingImage.setColor(ImageColor.COLOR);
        existingImage.setTheme(ImageTheme.valueOf(imageTheme));
        existingImage.setWidth(Integer.valueOf(imageWidth));
        existingImage.setHeight(Integer.valueOf(imageHeight));
    }

    private Optional<Image> checkForExistingImage(Set<Image> images, String imageTheme) {
        for (Image image : images) {
            if (image.getTheme().getName().equals(imageTheme)) {
                return Optional.of(image);
            }
        }
        return Optional.empty();
    }

    private Void deserializeAndUpdateChannel(HttpServletRequest req, HttpServletResponse resp) {
        Boolean strict = Boolean.valueOf(req.getParameter(STRICT));

        Optional<Application> possibleApplication;
        try {
            possibleApplication = appConfigFetcher.applicationFor(req);
        } catch (InvalidApiKeyException ex) {
            return error(req, resp, AtlasErrorSummary.forException(ex));
        }

        if (!possibleApplication.isPresent()) {
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new UnauthorizedException(
                            "API key is unauthorised"
                    ))
            );
        }

        Channel channel;
        try {
            channel = channelTransformer.transform(
                    deserialize(new InputStreamReader(req.getInputStream()), strict)
            );
        } catch (UnrecognizedPropertyException |
                JsonParseException |
                ConstraintViolationException e) {
            return error(req, resp, AtlasErrorSummary.forException(e));

        } catch (IOException ioe) {
            log.error("Error reading input for request " + req.getRequestURL(), ioe);
            return error(req, resp, AtlasErrorSummary.forException(ioe));

        } catch (Exception e) {
            log.error("Error reading input for request " + req.getRequestURL(), e);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withMessage("Error reading input for the request")
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);
        }

        if (!possibleApplication.get().getConfiguration().isWriteEnabled(channel.getSource())) {
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new ForbiddenException(
                            "API key does not have write permission"
                    ))
            );
        }

        try {
            store.createOrUpdate(channel);
        } catch (Exception e) {
            log.error("Error while creating/updating channel for request " + req.getRequestURL(), e);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);
        }

        resp.setStatus(HttpStatusCode.OK.code());
        resp.setContentLength(0);
        return null;
    }

    private org.atlasapi.media.entity.simple.Channel deserialize(Reader input, Boolean strict)
            throws IOException, ReadException {
        return reader.read(new BufferedReader(input), org.atlasapi.media.entity.simple.Channel.class, strict);
    }

    private Void error(
            HttpServletRequest request,
            HttpServletResponse response,
            AtlasErrorSummary summary
    ) {
        try {
            outputter.writeError(request, response, summary);
        } catch (IOException e) {
        }
        return null;
    }

    public static class ImageDetails {

        private String channelId;
        private String imageUri;
        private String imageTheme;
        private String imageHeight;
        private String imageWidth;

        public ImageDetails() {}

        public String getChannelId() {
            return channelId;
        }

        public void setChannelId(String channelId) {
            this.channelId = channelId;
        }

        public String getImageUri() {
            return imageUri;
        }

        public void setImageUri(String imageUri) {
            this.imageUri = imageUri;
        }

        public String getImageTheme() {
            return imageTheme;
        }

        public void setImageTheme(String imageTheme) {
            this.imageTheme = imageTheme;
        }

        public String getImageHeight() {
            return imageHeight;
        }

        public void setImageHeight(String imageHeight) {
            this.imageHeight = imageHeight;
        }

        public String getImageWidth() {
            return imageWidth;
        }

        public void setImageWidth(String imageWidth) {
            this.imageWidth = imageWidth;
        }

    }
}