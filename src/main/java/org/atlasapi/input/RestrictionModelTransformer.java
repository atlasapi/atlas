package org.atlasapi.input;

import org.atlasapi.media.entity.Restriction;

public class RestrictionModelTransformer {

    private RestrictionModelTransformer() {

    }

    public static RestrictionModelTransformer create() {
        return new RestrictionModelTransformer();
    }

    public Restriction transform(org.atlasapi.media.entity.simple.Restriction simple) {
        Restriction restriction = new Restriction();

        restriction.setRestricted(simple.isRestricted());
        restriction.setAuthority(simple.getAuthority());
        restriction.setRating(simple.getRating());
        restriction.setMinimumAge(simple.getMinimumAge());
        restriction.setMessage(simple.getMessage());
        return restriction;
    }


}
