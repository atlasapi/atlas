package org.atlasapi.remotesite.channel4;

import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public final class RecordingContentWriter implements ContentWriter {

	public final List<Item> updatedItems = Lists.newArrayList();
	public final List<Brand> updatedBrands = Lists.newArrayList();
	public final List<Series> updatedSeries = Lists.newArrayList();

	@Override
	public Item createOrUpdate(Item item) {
		updatedItems.add(item);
        return item;
	}

	@Override
	public void createOrUpdate(Container container) {
		if (container instanceof Brand) {
			updatedBrands.add((Brand) container);
		} else if (container instanceof Series) {
			updatedSeries.add((Series) container);
		} else {
			throw new IllegalArgumentException("Unknown container type: " + container);
		}
	}
	
	@Override
	public String toString() {
	    return Objects.toStringHelper(this)
	        .add("brands", updatedBrands)
	        .add("series", updatedSeries)
	        .add("items", updatedItems)
	   .toString();
	}
}
