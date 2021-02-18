package org.atlasapi.input;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;

import java.util.Set;

public class BrandModelTransformer extends ContentModelTransformer<Playlist, Brand> {

	private final EncodingModelTransformer encodingTransformer;
	private final RestrictionModelTransformer restrictionTransformer;

	public BrandModelTransformer(LookupEntryStore lookupStore, 
			TopicStore topicStore, NumberToShortStringCodec idCodec, 
			ClipModelTransformer clipsModelTransformer, Clock clock,
			ImageModelTransformer imageModelTransformer) {

		super(lookupStore, topicStore, idCodec, clipsModelTransformer, clock, imageModelTransformer);
		this.encodingTransformer = EncodingModelTransformer.create();
		this.restrictionTransformer = RestrictionModelTransformer.create();
	}

    @Override
    protected Brand setFields(Brand result, Playlist inputContent) {
        super.setFields(result, inputContent);
		DateTime now = this.clock.now();
        result.setCountriesOfOrigin(inputContent.getCountriesOfOrigin());

		Version version = new Version();
		Set<Encoding> encodings = encodingsFrom(inputContent.getLocations(), now);
		if (!encodings.isEmpty()) {
			version.setLastUpdated(now);
			version.setManifestedAs(encodings);
		}
		addRestrictions(inputContent, version);
		result.setVersions(ImmutableSet.of(version));

        return result;
    }

	@Override
	protected Brand createOutput(Playlist simple) {
		return new Brand();
	}

	private void addRestrictions(org.atlasapi.media.entity.simple.Playlist inputContent, Version version) {
		// Since we are coalescing multiple locations each with possibly its own restriction there is
		// no good way decide which restriction to keep so we are keeping the first one
		for (org.atlasapi.media.entity.simple.Location location : inputContent.getLocations()) {
			if (location.getRestriction() != null) {
				Restriction restriction = restrictionTransformer.transform(location.getRestriction());
				version.setRestriction(restriction);
				return;
			}
		}
	}

	private Set<Encoding> encodingsFrom(Set<org.atlasapi.media.entity.simple.Location> locations, DateTime now) {
		ImmutableSet.Builder<Encoding> encodings = ImmutableSet.builder();
		for (org.atlasapi.media.entity.simple.Location simpleLocation : locations) {
			Encoding encoding = encodingTransformer.transform(simpleLocation, now);
			encodings.add(encoding);
		}
		return encodings.build();
	}
}
