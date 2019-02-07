package org.atlasapi.output.simple;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.Aliased;
import org.atlasapi.media.entity.simple.Audit;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.atlasapi.output.Annotation;

import java.math.BigInteger;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class IdentifiedModelSimplifier<F extends Identified, T extends Aliased>
        implements ModelSimplifier<F, T> {

    static final Function<Alias, org.atlasapi.media.entity.simple.Alias> TO_SIMPLE_ALIAS = a -> new org.atlasapi.media.entity.simple.Alias(
            a.getNamespace(),
            a.getValue()
    );
    protected final NumberToShortStringCodec idCodec;
    private final PublisherSimplifier publisherSimplifier = new PublisherSimplifier();

    protected IdentifiedModelSimplifier() {
        this(new SubstitutionTableNumberCodec());
    }

    protected IdentifiedModelSimplifier(NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
    }

    protected void copyIdentifiedAttributesTo(Identified identified, Aliased aliased,
            Set<Annotation> annotations) {

        aliased.setUri(identified.getCanonicalUri());
        if (identified.getId() != null) {
            aliased.setId(idCodec.encode(BigInteger.valueOf(identified.getId())));
        }

        if (annotations.contains(Annotation.DESCRIPTION)
                || annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
            aliased.setAliases(identified.getAliasUrls());
            aliased.setCurie(identified.getCurie());
        }

        if (annotations.contains(Annotation.V4_ALIASES)) {
            aliased.setV4Aliases(ImmutableSet.copyOf(identified.getAliases()
                    .stream()
                    .map(TO_SIMPLE_ALIAS::apply)
                    .collect(Collectors.toList())));
        }

        if (annotations.contains(Annotation.AUDIT)) {
            Audit audit = new Audit();
            audit.setLastUpdated(identified.getLastUpdated());
            audit.setEquivalenceLastUdpated(identified.getEquivalenceUpdate());
            aliased.setAudit(audit);
        }

        if (annotations.contains(Annotation.CUSTOM_FIELDS)) {
            aliased.setCustomFields(identified.getCustomFields());
        }
    }

    protected PublisherDetails toPublisherDetails(Publisher publisher) {
        return publisherSimplifier.simplify(publisher);
    }
}
