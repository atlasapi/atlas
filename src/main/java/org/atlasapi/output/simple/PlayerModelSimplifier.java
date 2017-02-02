package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Player;
import org.atlasapi.output.Annotation;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


public class PlayerModelSimplifier extends DescribedModelSimplifier<Player, org.atlasapi.media.entity.simple.Player> {

    public PlayerModelSimplifier(ImageSimplifier imageSimplifier) {
        super(imageSimplifier, SubstitutionTableNumberCodec.lowerCaseOnly(), null);
    }

    @Override
    public org.atlasapi.media.entity.simple.Player simplify(Player model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.Player simpleModel = new org.atlasapi.media.entity.simple.Player();
        copyBasicDescribedAttributes(model, simpleModel, annotations);
        
        return simpleModel;
    }

}
