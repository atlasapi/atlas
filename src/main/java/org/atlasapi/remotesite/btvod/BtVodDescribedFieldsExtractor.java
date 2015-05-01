package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;


public class BtVodDescribedFieldsExtractor {

    private static final String MUSIC_CATEGORY = "Music";

    private final ImageUriProvider imageUriProvider;
    
    public BtVodDescribedFieldsExtractor(ImageUriProvider imageUriProvider) {
        this.imageUriProvider = checkNotNull(imageUriProvider);
    }
    
    public void setDescribedFieldsFrom(BtVodEntry row, Described described) {
        described.setDescription(row.getDescription());
        described.setLongDescription(row.getPlproduct$longDescription());
        described.setImages(createImages(row));
        
        if (!described.getImages().isEmpty()) {
            described.setImage(Iterables.getOnlyElement(described.getImages()).getCanonicalUri());
        }
    }
    
    private Iterable<Image> createImages(BtVodEntry row) {
        
        if (!MUSIC_CATEGORY.equals(row.getBtproduct$productType())) {
            return ImmutableSet.of();
        }

        Optional<String> imageUri = imageUriProvider.imageUriFor(row.getGuid());
        
        if (!imageUri.isPresent() || Strings.isNullOrEmpty(imageUri.get())) {
            return ImmutableSet.of();
        }
        
        Image image = new Image(imageUri.get());
        image.setType(ImageType.PRIMARY);
        return ImmutableSet.of(image);
    }
}
