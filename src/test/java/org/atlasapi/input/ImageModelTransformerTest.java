package org.atlasapi.input;

import java.sql.Date;
import java.time.Instant;

import org.atlasapi.media.entity.ImageAspectRatio;
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.media.entity.simple.Image;

import com.metabroadcast.common.media.MimeType;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ImageModelTransformerTest {

    private ImageModelTransformer translator;

    @Before
    public void setUp() throws Exception {
        this.translator = new ImageModelTransformer();
    }

    @Test
    public void translatingImageFromSimpleModelToComplexModel() {
        Image simple = new Image();
        simple.setUri("http://metabroadcast.com/channel/post/test");
        simple.setColor("COLOR");
        simple.setTheme("DARK_OPAQUE");
        simple.setWidth(1080);
        simple.setHeight(720);
        simple.setAspectRatio("16x9");
        simple.setMimeType("image/jpeg");
        simple.setAvailabilityStart(Date.from(Instant.parse("2016-06-06T00:00:00Z")));
        simple.setAvailabilityEnd(Date.from(Instant.parse("2016-06-06T01:00:00Z")));

        org.atlasapi.media.entity.Image complex = translator.transform(simple);

        assertThat(complex.getColor(), is(ImageColor.COLOR));
        assertThat(complex.getTheme(), is(ImageTheme.DARK_OPAQUE));
        assertThat(complex.getWidth(), is(1080));
        assertThat(complex.getHeight(), is(720));
        assertThat(complex.getAspectRatio(), is(ImageAspectRatio.SIXTEEN_BY_NINE));
        assertThat(complex.getMimeType(), is(MimeType.IMAGE_JPG));
        assertTrue(complex.getAvailabilityStart() != null);
        assertTrue(complex.getAvailabilityEnd() != null);
    }
}