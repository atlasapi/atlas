package org.atlasapi.remotesite.barb.channels;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.input.ImageModelTranslator;
import org.atlasapi.media.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class BarbStringToChannelTransformerTest {

    private ChannelModelTransformer modelTransformer;
    private BarbStringToChannelTransformer barbTransformer;

    @Before
    public void setUp() throws Exception {

        modelTransformer = ChannelModelTransformer.create(
                SubstitutionTableNumberCodec.lowerCaseOnly(),
                mock(ImageModelTranslator.class)
        );
        barbTransformer = BarbStringToChannelTransformer.create(modelTransformer);
    }

    @Test
    public void transformsSingleChannel() throws Exception {
        String channelString = "1234, Test channel v1";

        Channel c = barbTransformer.transform(channelString);

        assertThat(c.getAliases().size(), is(1));
        assertThat(c.getAliases().iterator().next().getValue(), is("1234"));
        assertThat(c.getTitle(), is("Test channel v1"));

    }

    @Test
    public void transformsMultipleChannels() throws Exception {
        String channelStrings = " 1111 , Test 1 |2222,Test2|3333, Test 3";

        String[] splitChannels = channelStrings.split("\\|");
        assertThat(splitChannels.length, is(3));

        Channel[] channels = new Channel[3];

        for (int i = 0; i < splitChannels.length; i++) {
            channels[i] = barbTransformer.transform(splitChannels[i]);
        }

        assertThat(channels[0].getAliases().iterator().next().getValue(), is("1111"));
        assertThat(channels[0].getTitle(), is("Test 1"));

        assertThat(channels[1].getAliases().iterator().next().getValue(), is("2222"));
        assertThat(channels[1].getTitle(), is("Test2"));

        assertThat(channels[2].getAliases().iterator().next().getValue(), is("3333"));
        assertThat(channels[2].getTitle(), is("Test 3"));
    }

}