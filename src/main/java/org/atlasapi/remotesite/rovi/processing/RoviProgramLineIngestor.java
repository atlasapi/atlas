package org.atlasapi.remotesite.rovi.processing;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.rovi.RoviCanonicalUriGenerator.canonicalUriForProgram;

import java.nio.charset.Charset;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.rovi.RoviContentWriter;
import org.atlasapi.remotesite.rovi.indexing.IndexAccessException;
import org.atlasapi.remotesite.rovi.model.RoviProgramLine;
import org.atlasapi.remotesite.rovi.model.RoviShowType;
import org.atlasapi.remotesite.rovi.parsers.RoviLineParser;
import org.atlasapi.remotesite.rovi.populators.ContentPopulatorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;


public class RoviProgramLineIngestor extends RoviActionLineIngestor<RoviProgramLine, Content> {
    
    private final static Logger LOG = LoggerFactory.getLogger(RoviProgramLineIngestor.class); 
    
    private final Predicate<? super RoviProgramLine> shouldProcess;
    private final ContentResolver contentResolver;
    private final ContentPopulatorSupplier contentPopulator;

    public RoviProgramLineIngestor(RoviLineParser<RoviProgramLine> parser, Charset charset,
            Predicate<? super RoviProgramLine> shouldProcess,
            RoviContentWriter contentWriter,
            ContentResolver contentResolver,
            ContentPopulatorSupplier contentPopulator) {
        super(parser, charset, contentWriter);
        this.shouldProcess = checkNotNull(shouldProcess);
        this.contentResolver = checkNotNull(contentResolver);
        this.contentPopulator = checkNotNull(contentPopulator);
    }

    @Override
    protected Logger log() {
        return LOG;
    }

    @Override
    protected boolean shouldProcess(RoviProgramLine parsedLine) {
        return shouldProcess.apply(parsedLine);
    }

    @Override
    protected Content populateContent(Content content, RoviProgramLine parsedLine) throws IndexAccessException {
        RoviShowType showType = parsedLine.getShowType().get();
        
        if (!ContentFactory.hasCorrectType(content, showType)) {
            // The content type has been changed, need to create a new content with the correct type
            Content newTypeContent = ContentFactory.createContent(showType);
            // Need to copy descriptions from the original content because they aren't in the program line (but in an external file)
            newTypeContent.setLocalizedDescriptions(content.getLocalizedDescriptions());
            content = newTypeContent;
        }
        
        contentPopulator.populateFromProgramAndAuxiliary(content, parsedLine);
        return content;
    }

    @Override
    protected Content createContent(RoviProgramLine parsedLine) {
        Content created = ContentFactory.createContent(parsedLine.getShowType().get());
        
        if (created instanceof Item) {
            setExistentVersionIfItemExists(created, parsedLine);
        }
            
        return created;
    }

    
    /**
     * This method make sure that if we're re-ingesting a program that already exists in our database, 
     * it keeps its original Version and therefore all the broadcasts attached to it 
     * 
     * @param created - the empty content just created
     * @param parsedLine - the line containing program infos
     */
    private void setExistentVersionIfItemExists(Content created, RoviProgramLine parsedLine) {
        Optional<Content> existent = resolveContent(parsedLine);
        
        if (existent.isPresent() && existent.get() instanceof Item) {
            Set<Version> existentVersions = ((Item) existent.get()).getVersions();
            ((Item) created).setVersions(existentVersions);
        }
    }

    @Override
    protected Optional<Content> resolveContent(RoviProgramLine parsedLine) {
        String canonicalUri = canonicalUriForProgram(parsedLine.getProgramId());
        Maybe<Identified> maybeResolved = contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUri)).getFirstValue();
        
        if (maybeResolved.isNothing()) {
            return Optional.absent();
        }
        
        return Optional.of((Content) maybeResolved.requireValue());
    }

}
