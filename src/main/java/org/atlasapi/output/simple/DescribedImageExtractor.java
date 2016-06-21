package org.atlasapi.output.simple;

import static com.google.common.base.Preconditions.checkArgument;
import static com.metabroadcast.common.media.MimeType.IMAGE_JPG;
import static org.atlasapi.media.entity.ImageAspectRatio.SIXTEEN_BY_NINE;
import static org.atlasapi.media.entity.ImageType.PRIMARY;

import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Projects a <code>Set&lt;{@link Image}&gt;</code> from a {@link Described} for
 * simplification into a {@link org.atlasapi.media.entity.simple.Description
 * Description}.
 * 
 * If there are no Images present and the image field is set then an Image is
 * synthesized from that field. If the Described is from a recognized
 * {@link Publisher} then the synthesized Image is populated with default
 * properties for that Publisher.
 * 
 */
public class DescribedImageExtractor {
    
    private final ImmutableMap<Publisher, Image> defaults
        = ImmutableMap.<Publisher,Image>builder()
            .put(Publisher.BBC, Image.builder("default")
                    .withWidth(640).withHeight(360)
                    .withAspectRatio(SIXTEEN_BY_NINE)
                    .withType(PRIMARY).withMimeType(IMAGE_JPG).build())
            .put(Publisher.PREVIEW_NETWORKS, Image.builder("default")
                    .withAspectRatio(SIXTEEN_BY_NINE)
                    .withType(PRIMARY).withMimeType(IMAGE_JPG).build())
            .build();
    
    public Set<Image> getImages(Described described) {
        if (!described.getImages().isEmpty()) {
            return described.getImages();
        }
        if (Strings.isNullOrEmpty(described.getImage())) {
            return ImmutableSet.of();
        }
        return ImmutableSet.of(createImage(described));
    }

    private Image createImage(Described described) {
        checkArgument(!Strings.isNullOrEmpty(described.getImage()),
                "Can't create Image from null/empty image field on %s", described);
        Image dflt = defaults.get(described.getPublisher());
        if (dflt == null) {
            Image image = new Image(described.getImage());
            image.setType(PRIMARY);
            return image;
        }
        return Image.builder(dflt).withUri(described.getImage()).build();
    }

}
