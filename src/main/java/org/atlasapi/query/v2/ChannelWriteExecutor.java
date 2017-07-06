package org.atlasapi.query.v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;

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

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.Sets;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelWriteExecutor {

    private static final String STRICT = "strict";

    private final Logger log = LoggerFactory.getLogger(ChannelWriteExecutor.class);
    private final ApplicationFetcher appConfigFetcher;
    private final ChannelStore store;
    private final ModelReader reader;
    private final ChannelModelTransformer channelTransformer;
    private final AtlasModelWriter<Iterable<Channel>> outputter;
    private final NumberToShortStringCodec codec;

    private ChannelWriteExecutor(Builder builder) {
        this.appConfigFetcher = checkNotNull(builder.appConfigFetcher);
        this.store = checkNotNull(builder.store);
        this.reader = checkNotNull(builder.reader);
        this.channelTransformer = checkNotNull(builder.channelTransformer);
        this.outputter = checkNotNull(builder.outputter);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    public static Builder builder() {
        return new Builder();
    }

    @RequestMapping(value = "/3.0/channels", method = RequestMethod.POST)
    public Void postChannel(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateChannel(req, resp);
    }

    public Void createOrUpdateChannelImage(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ImageDetails imageDetails = mapper.readValue(request.getInputStream(), ImageDetails.class);

        Optional<Void> invalidApplication = validateApplication(request, response);
        if (invalidApplication.isPresent()) {
            return invalidApplication.get();
        }

        String channelId = imageDetails.getChannelId();
        String imageTheme = imageDetails.getTheme();

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

        if (Strings.isNullOrEmpty(imageDetails.getUri())
                || Strings.isNullOrEmpty(imageDetails.getHeight())
                || Strings.isNullOrEmpty(imageDetails.getWidth())
                || Strings.isNullOrEmpty(imageDetails.getMimeType())) {
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

        createOrUpdateImage(existingChannel, imageDetails);

        try {
            store.createOrUpdate(existingChannel);
        } catch (Exception e) {
            log.error("Error while updating channel for request {}", request.getRequestURL(), e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            return error(request, response, AtlasErrorSummary.forException(e));
        }

        response.setStatus(HttpServletResponse.SC_OK);
        return null;

    }

    private Optional<Void> validateApplication(HttpServletRequest request, HttpServletResponse response) {
        Optional<Application> possibleApplication;
        try {
            possibleApplication = appConfigFetcher.applicationFor(request);
        } catch (InvalidApiKeyException ex) {
            return Optional.of(error(request, response, AtlasErrorSummary.forException(ex)));
        }

        if (!possibleApplication.isPresent()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return Optional.of(error(
                    request,
                    response,
                    AtlasErrorSummary.forException(
                            new UnauthorizedException(
                                    "API key is unauthorised"
                            )
                    )
            ));
        }
        return Optional.empty();
    }

    public Void deleteChannelImage(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ImageDetails imageDetails = mapper.readValue(request.getInputStream(), ImageDetails.class);

        Optional<Void> invalidApplication = validateApplication(request, response);
        if (invalidApplication.isPresent()) {
            return invalidApplication.get();
        }

        String channelId = imageDetails.getChannelId();
        String imageTheme = imageDetails.getTheme();

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

        if (Strings.isNullOrEmpty(imageDetails.getUri())) {
            deleteImage(imageTheme, existingChannel);
        }

        try {
            store.createOrUpdate(existingChannel);
        } catch (Exception e) {
            log.error("Error while updating channel for request {}", request.getRequestURL(), e);
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

    private void createOrUpdateImage(Channel existingChannel, ImageDetails imageDetails) {
        Set<Image> channelImages = Sets.newHashSet(existingChannel.getImages());
        Optional<Image> possibleImage = getPossibleImageByTheme(channelImages, imageDetails.getTheme());

        if (possibleImage.isPresent()) {
            Image imageToBeUpdated = possibleImage.get();

            channelImages.remove(imageToBeUpdated);

            Image updatedImage = updateImage(imageDetails, imageToBeUpdated);

            channelImages.add(updatedImage);

            setImagesOnChannel(existingChannel, channelImages);
        } else {
            Image newImage = createImage(imageDetails);

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

    private Image createImage(ImageDetails imageDetails) {
        Image newImage = new Image(imageDetails.getUri());
        setImageDetails(imageDetails, newImage);

        return newImage;
    }

    private Image updateImage(ImageDetails imageDetails, Image imageToBeUpdated) {
        imageToBeUpdated.setCanonicalUri(imageDetails.getUri());
        setImageDetails(imageDetails, imageToBeUpdated);

        return imageToBeUpdated;
    }

    private void setImageDetails(ImageDetails imageDetails, Image existingImage) {
        existingImage.setMimeType(MimeType.valueOf(imageDetails.getMimeType().toUpperCase()));
        existingImage.setType(ImageType.LOGO);
        existingImage.setColor(ImageColor.MONOCHROME);
        existingImage.setTheme(ImageTheme.valueOf(imageDetails.getTheme().toUpperCase()));
        existingImage.setWidth(Integer.valueOf(imageDetails.getWidth()));
        existingImage.setHeight(Integer.valueOf(imageDetails.getHeight()));
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
        private String uri;
        private String theme;
        private String mimeType;
        private String height;
        private String width;

        public ImageDetails() {
        }

        public String getChannelId() {
            return channelId;
        }

        public void setChannelId(String channelId) {
            this.channelId = channelId;
        }

        public String getUri() {
            return uri;
        }

        public void setUri(String imageUri) {
            this.uri = imageUri;
        }

        public String getTheme() {
            return theme;
        }

        public void setTheme(String theme) {
            this.theme = theme;
        }

        public String getMimeType() {
            return mimeType;
        }

        public void setMimeType(String mimeType) {
            this.mimeType = mimeType;
        }

        public String getHeight() {
            return height;
        }

        public void setHeight(String height) {
            this.height = height;
        }

        public String getWidth() {
            return width;
        }

        public void setWidth(String width) {
            this.width = width;
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
