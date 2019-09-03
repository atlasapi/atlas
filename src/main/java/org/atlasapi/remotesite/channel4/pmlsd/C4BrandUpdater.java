package org.atlasapi.remotesite.channel4.pmlsd;

import org.atlasapi.media.entity.Brand;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;

//Why is this here? Isn't it just a SiteSpecificAdapter<Brand>?
public interface C4BrandUpdater {
	
	Brand createOrUpdateBrand(ModelWithPayload<String> brandUriWithPayload);

	boolean canFetch(String uri);

}
