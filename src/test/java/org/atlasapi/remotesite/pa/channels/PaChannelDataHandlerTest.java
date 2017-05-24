package org.atlasapi.remotesite.pa.channels;

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
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PaChannelDataHandlerTest {

    private Channel existingChannel;
    private Channel newChannel;
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

        Image existingImage = Image.builder("existing uri").withTheme(ImageTheme.DARK_MONOCHROME).build();
        existingChannel = Channel.builder().withImage(existingImage).build();
    }

    @Test
    public void dontUpdateImagesIfNewChannelDoesntHaveImages() {
        newChannel = Channel.builder().build();

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        assertTrue(Iterables.size(existingChannel.getAllImages()) == 1);
    }

    @Test
    public void updateExistingImageDetailsIfImagesThemeMatch() {
        Image newImage = Image.builder("new uri").withTheme(ImageTheme.DARK_MONOCHROME).build();
        newChannel = Channel.builder().withImage(newImage).build();

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        assertTrue(Iterables.size(existingChannel.getAllImages()) == 1);
        assertEquals(existingChannel.getAllImages().iterator().next().getValue().getCanonicalUri(), "new uri");
    }

    @Test
    public void addNewImageIfItDoesntExist() {
        Image newImage = Image.builder("new uri").withTheme(ImageTheme.LIGHT_MONOCHROME).build();
        newChannel = Channel.builder().build();
        newChannel.addImage(newImage, now.plusDays(1), now.plusDays(5));

        existingChannel.setImages(channelDataHandler.updateImages(newChannel, existingChannel));

        Set<TemporalField<Image>> images = Sets.newHashSet(existingChannel.getAllImages());

        assertTrue(Iterables.size(existingChannel.getAllImages()) == 2);

        images.forEach(image -> {
            if (newImage.getCanonicalUri().equals(image.getValue().getCanonicalUri())) {
                assertThat(image.getStartDate(), is(now.plusDays(1)));
                assertThat(image.getEndDate(), is(now.plusDays(5)));
            }
        });


    }
}
