package org.atlasapi.equiv.update.metadata;

import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class SourceSpecificEquivalenceUpdaterMetadata extends EquivalenceUpdaterMetadata {

    private final String source;
    private final EquivalenceUpdaterMetadata topLevelContainerUpdaterMetadata;
    private final EquivalenceUpdaterMetadata nonTopLevelContainerUpdaterMetadata;
    private final EquivalenceUpdaterMetadata itemUpdaterMetadata;

    private SourceSpecificEquivalenceUpdaterMetadata(
            String source,
            EquivalenceUpdaterMetadata topLevelContainerUpdaterMetadata,
            EquivalenceUpdaterMetadata nonTopLevelContainerUpdaterMetadata,
            EquivalenceUpdaterMetadata itemUpdaterMetadata
    ) {
        this.source = checkNotNull(source);
        this.topLevelContainerUpdaterMetadata = checkNotNull(topLevelContainerUpdaterMetadata);
        this.nonTopLevelContainerUpdaterMetadata = checkNotNull(
                nonTopLevelContainerUpdaterMetadata
        );
        this.itemUpdaterMetadata = checkNotNull(itemUpdaterMetadata);
    }

    public static SourceStep builder() {
        return new Builder();
    }

    public String getSource() {
        return source;
    }

    public EquivalenceUpdaterMetadata getTopLevelContainerUpdaterMetadata() {
        return topLevelContainerUpdaterMetadata;
    }

    public EquivalenceUpdaterMetadata getNonTopLevelContainerUpdaterMetadata() {
        return nonTopLevelContainerUpdaterMetadata;
    }

    public EquivalenceUpdaterMetadata getItemUpdaterMetadata() {
        return itemUpdaterMetadata;
    }

    public interface SourceStep {

        TopLevelContainerUpdaterMetadataStep withSource(Publisher source);
    }

    public interface TopLevelContainerUpdaterMetadataStep {

        NonTopLevelContainerUpdaterMetadataStep withTopLevelContainerUpdaterMetadata(
                EquivalenceUpdaterMetadata topLevelContainerUpdaterMetadata
        );
    }

    public interface NonTopLevelContainerUpdaterMetadataStep {

        ItemUpdaterMetadataStep withNonTopLevelContainerUpdaterMetadata(
                EquivalenceUpdaterMetadata nonTopLevelContainerUpdaterMetadata
        );
    }

    public interface ItemUpdaterMetadataStep {

        BuildStep withItemUpdaterMetadata(EquivalenceUpdaterMetadata itemUpdaterMetadata);
    }

    public interface BuildStep {

        SourceSpecificEquivalenceUpdaterMetadata build();
    }

    public static class Builder implements SourceStep, TopLevelContainerUpdaterMetadataStep,
            NonTopLevelContainerUpdaterMetadataStep, ItemUpdaterMetadataStep, BuildStep {

        private String source;
        private EquivalenceUpdaterMetadata topLevelContainerUpdaterMetadata;
        private EquivalenceUpdaterMetadata nonTopLevelContainerUpdaterMetadata;
        private EquivalenceUpdaterMetadata itemUpdaterMetadata;

        private Builder() {
        }

        @Override
        public TopLevelContainerUpdaterMetadataStep withSource(Publisher source) {
            this.source = source.key();
            return this;
        }

        @Override
        public NonTopLevelContainerUpdaterMetadataStep withTopLevelContainerUpdaterMetadata(
                EquivalenceUpdaterMetadata topLevelContainerUpdaterMetadata) {
            this.topLevelContainerUpdaterMetadata = topLevelContainerUpdaterMetadata;
            return this;
        }

        @Override
        public ItemUpdaterMetadataStep withNonTopLevelContainerUpdaterMetadata(
                EquivalenceUpdaterMetadata nonTopLevelContainerUpdaterMetadata) {
            this.nonTopLevelContainerUpdaterMetadata = nonTopLevelContainerUpdaterMetadata;
            return this;
        }

        @Override
        public BuildStep withItemUpdaterMetadata(EquivalenceUpdaterMetadata itemUpdaterMetadata) {
            this.itemUpdaterMetadata = itemUpdaterMetadata;
            return this;
        }

        @Override
        public SourceSpecificEquivalenceUpdaterMetadata build() {
            return new SourceSpecificEquivalenceUpdaterMetadata(
                    this.source,
                    this.topLevelContainerUpdaterMetadata,
                    this.nonTopLevelContainerUpdaterMetadata,
                    this.itemUpdaterMetadata
            );
        }
    }
}
