package org.atlasapi.query.v2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.stream.MoreCollectors;
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
import org.atlasapi.media.entity.simple.response.WriteResponse;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.exceptions.ForbiddenException;
import org.atlasapi.output.exceptions.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    public WriteResponse postChannel(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateChannel(req, resp, true);
    }

    public WriteResponse putChannel(HttpServletRequest req, HttpServletResponse resp) {
        return deserializeAndUpdateChannel(req, resp, false);
    }

    @Nullable
    public WriteResponse createOrUpdateChannelImage(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ImageDetails imageDetails = mapper.readValue(request.getInputStream(), ImageDetails.class);

        PossibleApplication possibleApplication = validateApplicationConfiguration(request);
        if (possibleApplication.getErrorSummary().isPresent()) {
            return error(request, response, possibleApplication.getErrorSummary().get());
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
            Channel savedChannel = store.createOrUpdate(existingChannel);
            response.setStatus(HttpServletResponse.SC_OK);
            return new WriteResponse(encodeId(savedChannel.getId()));
        } catch (Exception e) {
            log.error("Error while updating channel for request {}", request.getRequestURL(), e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            return error(request, response, AtlasErrorSummary.forException(e));
        }
    }

    @Nonnull
    private PossibleApplication validateApplicationConfiguration(HttpServletRequest request) {
        PossibleApplication possibleApplication = new PossibleApplication();
        try {
            possibleApplication.setApplication(appConfigFetcher.applicationFor(request).orElse(null));
        } catch (InvalidApiKeyException ex) {
            possibleApplication.setErrorSummary(AtlasErrorSummary.forException(ex));
        }

        if (!possibleApplication.getApplication().isPresent()) {
            possibleApplication.setErrorSummary(
                    AtlasErrorSummary.forException(new UnauthorizedException(
                            "API key is unauthorised"
                    ))
            );
        }

        return possibleApplication;
    }

    //this is knowlngly ugly cause we didn't think it was worth doing anything prettier. Don't
    //use it a good example.
    private class PossibleApplication {

        private AtlasErrorSummary errorSummary;
        private Application application;

        @Nonnull
        public Optional<AtlasErrorSummary> getErrorSummary() {
            return Optional.ofNullable(errorSummary);
        }

        public void setErrorSummary(@Nullable AtlasErrorSummary errorSummary) {
            this.errorSummary = errorSummary;
        }

        @Nonnull
        public Optional<Application> getApplication() {
            return Optional.ofNullable(application);
        }

        public void setApplication(@Nullable Application application) {
            this.application = application;
        }
    }

    @Nullable
    public WriteResponse deleteChannelImage(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ImageDetails imageDetails = mapper.readValue(request.getInputStream(), ImageDetails.class);

        PossibleApplication possibleApplication = validateApplicationConfiguration(request);
        if (possibleApplication.getErrorSummary().isPresent()) {
            return error(request, response, possibleApplication.getErrorSummary().get());
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
            Channel savedChannel = store.createOrUpdate(existingChannel);
            response.setStatus(HttpServletResponse.SC_OK);
            return new WriteResponse(encodeId(savedChannel.getId()));
        } catch (Exception e) {
            log.error("Error while updating channel for request {}", request.getRequestURL(), e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            return error(request, response, AtlasErrorSummary.forException(e));
        }
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
        existingImage.setMimeType(MimeType.fromString(imageDetails.getMimeType().toUpperCase()));
        existingImage.setType(ImageType.LOGO);
        existingImage.setColor(ImageColor.MONOCHROME);
        existingImage.setTheme(ImageTheme.valueOf(imageDetails.getTheme().toUpperCase()));
        existingImage.setWidth(Integer.valueOf(imageDetails.getWidth()));
        existingImage.setHeight(Integer.valueOf(imageDetails.getHeight()));
    }

    @Nullable
    private WriteResponse deserializeAndUpdateChannel(HttpServletRequest req, HttpServletResponse resp, boolean merge) {
        Boolean strict = Boolean.valueOf(req.getParameter(STRICT));

        PossibleApplication possibleApplication = validateApplicationConfiguration(req);
        if (possibleApplication.getErrorSummary().isPresent()) {
            return error(req, resp, possibleApplication.getErrorSummary().get());
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

        if (!possibleApplication.getApplication().get().getConfiguration().isWriteEnabled(channel.getSource())) {
            return error(
                    req,
                    resp,
                    AtlasErrorSummary.forException(new ForbiddenException(
                            "API key does not have write permission"
                    ))
            );
        }

        if (merge) {
            Optional<Channel> existingChannel = store.fromUri(channel.getCanonicalUri()).toOptional();
            if (existingChannel.isPresent()) {
                mergeChannel(channel, existingChannel.get());
            }
        }

        try {
            Channel savedChannel = store.createOrUpdate(channel);
            resp.setStatus(HttpStatus.OK.value());
            return new WriteResponse(encodeId(savedChannel.getId()));
        } catch (Exception e) {
            log.error("Error while creating/updating channel for request {}", req.getRequestURL(), e);
            AtlasErrorSummary errorSummary = new AtlasErrorSummary(e)
                    .withStatusCode(HttpStatusCode.BAD_REQUEST);
            return error(req, resp, errorSummary);
        }

    }

    // There used to be no merging done at all, with the POST endpoint as the only endpoint
    // The merging is kept minimal for now since the only current use case is to preserve BT custom channel logos
    // and custom channel groups on api.youview.tv channels.
    private void mergeChannel(Channel newChannel, Channel existingChannel) {
        newChannel.setImages(mergeImages(newChannel.getAllImages(), existingChannel.getAllImages()));
        if (newChannel.getChannelNumbers().isEmpty()) {
            newChannel.setChannelNumbers(existingChannel.getChannelNumbers());
        }
    }

    // Update the existing channel images to avoid overwriting all existing images, in case the theme
    // comes from elsewhere, e.g. from BT using the channel logo tool.
    private Iterable<TemporalField<Image>> mergeImages(
            Iterable<TemporalField<Image>> newImages,
            Iterable<TemporalField<Image>> existingImages
    ) {
        Set<ImageTheme> newThemes = StreamSupport
                .stream(newImages.spliterator(), false)
                .map(image -> image.getValue().getTheme())
                .collect(MoreCollectors.toImmutableSet());

        Set<TemporalField<Image>> preservedImages = StreamSupport
                .stream(existingImages.spliterator(), false)
                .filter(image -> !newThemes.contains(image.getValue().getTheme()))
                .collect(MoreCollectors.toImmutableSet());

        if (preservedImages.isEmpty()) {
            return newImages;
        }

        return Sets.union(ImmutableSet.copyOf(newImages), preservedImages);

    }

    private String encodeId(Long contentId) {
        return codec.encode(BigInteger.valueOf(contentId));
    }

    private org.atlasapi.media.entity.simple.Channel deserialize(Reader input, Boolean strict)
            throws IOException, ReadException {
        return reader.read(new BufferedReader(input), org.atlasapi.media.entity.simple.Channel.class, strict);
    }

    @Nullable
    private WriteResponse error(
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
