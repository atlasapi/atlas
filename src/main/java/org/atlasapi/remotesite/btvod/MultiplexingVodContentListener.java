package org.atlasapi.remotesite.btvod;

import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.collect.ImmutableList;

public class MultiplexingVodContentListener implements BtVodContentListener {

    private final ImmutableList<BtVodContentListener> delegates;

    public MultiplexingVodContentListener(List<BtVodContentListener> delegates) {
        this.delegates = ImmutableList.copyOf(delegates);
    }

    @Override
    public void onContent(Content content, BtVodEntry vodData) {
        for (BtVodContentListener delegate : delegates) {
            delegate.onContent(content, vodData);
        }
    }

    @Override
    public void beforeContent() {
        for (BtVodContentListener delegate : delegates) {
            delegate.beforeContent();
        }
    }

    @Override
    public void afterContent() {
        for (BtVodContentListener delegate : delegates) {
            delegate.afterContent();
        }
    }
}
