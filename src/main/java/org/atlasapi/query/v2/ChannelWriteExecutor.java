package org.atlasapi.query.v2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.Sets;
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
import org.atlasapi.media.channel.TemporalField;
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
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelWriteExecutor {

    private static final String STRICT = "strict";
    private final Logger log = LoggerFactory.getLogger(ChannelWriteExecutor.class);
    private final ApplicationFetcher appConfigFetcher;
    private final ChannelStore store;
    private final ModelReader reader;
    private final ChannelModelTransformer channelTransformer;
    private final AtlasModelWriter<Iterable<Channel>> outputter;

    private ChannelWriteExecutor(Builder builder) {
        this.appConfigFetcher = checkNotNull(builder.appConfigFetcher);
        this.store = checkNotNull(builder.store);
        this.reader = checkNotNull(builder.reader);
        this.channelTransformer = checkNotNull(builder.channelTransformer);
        this.outputter = checkNotNull(builder.outputter);
    }

    public static Builder builder() {
        return new Builder();
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

        String channelId = incomingJson.getChannelId();
        String imageTheme = incomingJson.getImageTheme();

        if (Strings.isNullOrEmpty(channelId) || Strings.isNullOrEmpty(imageTheme)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(
                            new IllegalArgumentException(
                                    "You must specify a channel ID and image theme to make a request."
                            )
                    )
            );
        }

        Maybe<Channel> possibleChannel = store.fromId(codec.decode(channelId).longValue());

        if (!possibleChannel.hasValue()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return error(
                    request,
                    response,
                    AtlasErrorSummary.forException(new NullPointerException(
                            String.format("No channel has been found for ID %s.", channelId)
                    ))
            );
        }

        Channel existingChannel = possibleChannel.requireValue();

        String imageUri = incomingJson.getImageUri();
        if (Strings.isNullOrEmpty(imageUri)) {
            deleteImage(imageTheme, existingChannel);
        } else {
            String imageHeight = incomingJson.getImageHeight();
            String imageWidth = incomingJson.getImageWidth();
            String imageMimeType = incomingJson.getImageMimeType();
            if (Strings.isNullOrEmpty(imageHeight)
                    || Strings.isNullOrEmpty(imageWidth)
                    || Strings.isNullOrEmpty(imageMimeType)) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return error(
                        request,
                        response,
                        AtlasErrorSummary.forException(
                                new IllegalArgumentException(
                                        "You must specify the height, width and mimeType in order to create/update an image."
                                )
                        )
                );
            }

            createOrUpdateImage(existingChannel, imageUri, imageTheme, imageMimeType, imageHeight, imageWidth);
        }

        try {
            store.createOrUpdate(existingChannel);
        } catch (Exception e) {
            log.error("Error while creating/updating channel for request {}", request.getRequestURL(), e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            return error(request, response, AtlasErrorSummary.forException(e));
        }

        response.setStatus(HttpServletResponse.SC_OK);
        return null;
    }

    private void deleteImage(String imageTheme, Channel existingChannel) {
        Set<Image> channelImages = Sets.newHashSet(existingChannel.getImages());

        Optional<Image> possibleImage = getPossibleImageByTheme(channelImages, imageTheme);

        if (possibleImage.isPresent()) {
            channelImages.remove(possibleImage.get());

            setImagesOnChannel(existingChannel, channelImages);
        } else {
            log.error("Image not found on channel {} for theme {}", existingChannel.getId(), imageTheme);
        }
    }

    private Optional<Image> getPossibleImageByTheme(Set<Image> channelImages, String imageTheme) {
        return channelImages.stream()
                .filter(existingImage -> existingImage.getTheme().equals(ImageTheme.valueOf(imageTheme.toUpperCase())))
                .findFirst();
    }

    private void createOrUpdateImage(
            Channel existingChannel,
            String imageUri,
            String imageTheme,
            String imageMimeType,
            String imageHeight,
            String imageWidth
    ) {
        Set<Image> channelImages = Sets.newHashSet(existingChannel.getImages());
        Optional<Image> possibleImage = getPossibleImageByTheme(channelImages, imageTheme);

        if (possibleImage.isPresent()) {
            Image imageToBeUpdated = possibleImage.get();

            channelImages.remove(imageToBeUpdated);

            Image updatedImage = updateImage(
                    imageUri,
                    imageTheme,
                    imageMimeType,
                    imageHeight,
                    imageWidth,
                    imageToBeUpdated
            );

            channelImages.add(updatedImage);

            setImagesOnChannel(existingChannel, channelImages);
        } else {
            Image newImage = createImage(imageUri, imageTheme, imageMimeType, imageHeight, imageWidth);

            existingChannel.addImage(newImage);
        }
    }

    private void setImagesOnChannel(Channel existingChannel, Set<Image> channelImages) {
        existingChannel.setImages(
                channelImages.stream()
                        .map(image -> new TemporalField<>(image, null, null))
                        .collect(Collectors.toSet())
        );
    }

    private Image createImage(
            String imageUri,
            String imageTheme,
            String imageMimeType,
            String imageHeight,
            String imageWidth
    ) {
        Image newImage = new Image(imageUri);
        setImageDetails(imageMimeType, imageTheme, imageHeight, imageWidth, newImage);

        return newImage;
    }

    private Image updateImage(
            String imageUri,
            String imageTheme,
            String imageMimeType,
            String imageHeight,
            String imageWidth,
            Image imageToBeUpdated
    ) {
        imageToBeUpdated.setCanonicalUri(imageUri);
        setImageDetails(imageMimeType, imageTheme, imageHeight, imageWidth, imageToBeUpdated);

        return imageToBeUpdated;
    }

    private void setImageDetails(
            String mimeType,
            String imageTheme,
            String imageHeight,
            String imageWidth,
            Image existingImage
    ) {
        existingImage.setMimeType(MimeType.valueOf(mimeType.toUpperCase()));
        existingImage.setType(ImageType.LOGO);
        existingImage.setColor(ImageColor.MONOCHROME);
        existingImage.setTheme(ImageTheme.valueOf(imageTheme.toUpperCase()));
        existingImage.setWidth(Integer.valueOf(imageWidth));
        existingImage.setHeight(Integer.valueOf(imageHeight));
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
        private String imageMimeType;
        private String imageHeight;
        private String imageWidth;

        public ImageDetails() {
        }

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

        public String getImageMimeType() {
            return imageMimeType;
        }

        public void setImageMimeType(String imageMimeType) {
            this.imageMimeType = imageMimeType;
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

    public static class Builder {

        private ApplicationFetcher appConfigFetcher;
        private ChannelStore store;
        private ModelReader reader;
        private ChannelModelTransformer channelTransformer;
        private AtlasModelWriter<Iterable<Channel>> outputter;

        private Builder() {
        }

        public Builder withAppConfigFetcher(ApplicationFetcher appConfigFetcher) {
            this.appConfigFetcher = appConfigFetcher;
            return this;
        }

        public Builder withChannelStore(ChannelStore store) {
            this.store = store;
            return this;
        }

        public Builder withModelReader(ModelReader reader) {
            this.reader = reader;
            return this;
        }

        public Builder withChannelTransformer(ChannelModelTransformer channelTransformer) {
            this.channelTransformer = channelTransformer;
            return this;
        }

        public Builder withOutputter(AtlasModelWriter<Iterable<Channel>> outputter) {
            this.outputter = outputter;
            return this;
        }

        public ChannelWriteExecutor build() {
            return new ChannelWriteExecutor(this);
        }
    }
}
