package org.atlasapi.remotesite.pa.channels;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.google.common.collect.Sets;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageTheme;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PaChannelDataHandlerTest {

    private Channel existingChannel;
    private Channel newChannel;
    private TemporalField<Image> existingImage;
    private PaChannelDataHandler channelDataHandler;
    private LocalDate now;

    @Before
    public void setUp() {
        PaChannelsIngester paChannelsIngester = mock(PaChannelsIngester.class);
        PaChannelGroupsIngester paChannelGroupsIngester = mock(PaChannelGroupsIngester.class);
        ChannelResolver channelResolver = mock(ChannelResolver.class);
        ChannelWriter channelWriter = mock(ChannelWriter.class);
        ChannelGroupResolver channelGroupResolver = mock(ChannelGroupResolver.class);
        ChannelGroupWriter channelGroupWriter = mock(ChannelGroupWriter.class);

        channelDataHandler = new PaChannelDataHandler(
                paChannelsIngester,
                paChannelGroupsIngester,
                channelResolver,
                channelWriter,
                channelGroupResolver,
                channelGroupWriter
        );

        now = LocalDate.now();

        newChannel = Channel.builder().build();
        existingChannel = Channel.builder().build();

        existingImage = makeTemporalImage("existingUri", ImageTheme.DARK_MONOCHROME, null, null);
        existingChannel.setImages(ImmutableSet.of(existingImage));
    }

    @Test
    public void dontUpdateImagesIfNewChannelDoesntHaveImages() {
        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        assertTrue(Iterables.size(existingChannel.getAllImages()) == 1);
    }

    @Test
    public void updateExistingImageDetailsIfImagesThemeMatch() {
        TemporalField<Image> newImage = makeTemporalImage("newUri", ImageTheme.DARK_MONOCHROME, null, null);

        newChannel.setImages(ImmutableSet.of(newImage));

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        Set<TemporalField<Image>> images = Sets.newHashSet(existingChannel.getAllImages());

        assertThat(images.size(), is(1));
        assertThat(Iterables.getOnlyElement(images), is(newImage));
    }

    @Test
    public void addNewImageIfItDoesntExist() {
        TemporalField<Image> newImage = makeTemporalImage("newImage", ImageTheme.LIGHT_MONOCHROME, null, null);

        newChannel.setImages(ImmutableSet.of(newImage));

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        Set<TemporalField<Image>> images = Sets.newHashSet(existingChannel.getAllImages());

        assertThat(images.size(), is(2));
        assertTrue(images.contains(newImage));
        assertTrue(images.contains(existingImage));
    }

    @Test
    public void existingImageIsUpdatedWithEndDate() {
        TemporalField<Image> existingGood = makeTemporalImage("existingGood", ImageTheme.LIGHT_OPAQUE, now.minusMonths(1), null);

        TemporalField<Image> existingGoodUpdated = makeTemporalImage("existingGood", ImageTheme.LIGHT_OPAQUE, now.minusMonths(1), now.plusDays(2));
        TemporalField<Image> futureGoodReplacement = makeTemporalImage("futureGoodReplacement", ImageTheme.LIGHT_OPAQUE, now.plusDays(2), now.plusMonths(1));

        existingChannel.setImages(ImmutableSet.of(existingGood));
        newChannel.setImages(ImmutableSet.of(existingGoodUpdated, futureGoodReplacement));

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        Set<TemporalField<Image>> images = Sets.newHashSet(existingChannel.getAllImages());

        assertThat(images.size(), is(2));
        assertTrue(images.contains(existingGoodUpdated));
        assertTrue(images.contains(futureGoodReplacement));
    }

    @Test
    public void addsSameImageThemeIfDatesAreValid() {
        TemporalField<Image> badImage1 = makeTemporalImage("badImage1", ImageTheme.LIGHT_OPAQUE, null, null);
        TemporalField<Image> badImage2 = makeTemporalImage("badImage2", ImageTheme.LIGHT_OPAQUE, null, null);

        TemporalField<Image> goodImage1 = makeTemporalImage("goodImage1", ImageTheme.LIGHT_OPAQUE, now.minusMonths(1), now.minusDays(2));
        TemporalField<Image> goodImage2 = makeTemporalImage("goodImage2", ImageTheme.LIGHT_OPAQUE, now.minusDays(2), now.plusMonths(1));

        existingChannel.setImages(ImmutableSet.of(badImage1, badImage2));
        newChannel.setImages(ImmutableSet.of(goodImage1, goodImage2));

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        Set<TemporalField<Image>> images = Sets.newHashSet(existingChannel.getAllImages());

        assertThat(images.size(), is(2));
        assertTrue(images.contains(goodImage1));
        assertTrue(images.contains(goodImage2));
    }

    @Test
    public void badImagesAreStrippedFromChannelImagesAndReplacedWithGood() {
        TemporalField<Image> badPrimary1 = makeTemporalImage("primary1", ImageTheme.LIGHT_OPAQUE, null, null);
        TemporalField<Image> badPrimary2 = makeTemporalImage("primary2", ImageTheme.LIGHT_OPAQUE, null, null);

        TemporalField<Image> goodPrimary1 = makeTemporalImage("goodPrimary", ImageTheme.LIGHT_OPAQUE, now.minusDays(2), null);

        existingChannel.setImages(ImmutableSet.of(badPrimary1, badPrimary2));
        newChannel = Channel.builder().build();
        newChannel.setImages(ImmutableSet.of(goodPrimary1));

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        Set<TemporalField<Image>> images = Sets.newHashSet(existingChannel.getAllImages());

        assertThat(images.size(), is(1));
        assertThat(Iterables.getOnlyElement(images), is(goodPrimary1));

    }

    private TemporalField<Image> makeTemporalImage(
            String uri,
            ImageTheme theme,
            LocalDate start,
            LocalDate end
    ) {
        Image image = Image.builder(uri).withTheme(theme).build();
        return new TemporalField<>(image, start, end);
    }

}
