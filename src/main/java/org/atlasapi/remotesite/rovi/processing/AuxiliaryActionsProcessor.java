package org.atlasapi.remotesite.rovi.processing;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.rovi.RoviCanonicalUriGenerator.canonicalUriForProgram;

import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.rovi.RoviContentWriter;
import org.atlasapi.remotesite.rovi.indexing.IndexAccessException;
import org.atlasapi.remotesite.rovi.populators.ContentPopulatorSupplier;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;


public class AuxiliaryActionsProcessor {
    
    private final static Logger LOG = LoggerFactory.getLogger(AuxiliaryActionsProcessor.class);

    private final ContentResolver contentResolver;
    private final ContentPopulatorSupplier populator;
    private final RoviContentWriter contentWriter;
    
    public AuxiliaryActionsProcessor(
            ContentPopulatorSupplier populator,
            ContentResolver contentResolver,
            RoviContentWriter contentWriter) {
        this.contentResolver = checkNotNull(contentResolver);
        this.populator = checkNotNull(populator);
        this.contentWriter = checkNotNull(contentWriter);
    }
    
    public RoviDataProcessingResult process(Set<String> programIds) {
        DateTime startTime = now();
        int processedLines = 0;
        int failedLines = 0;
        
        for (String programId: programIds) {
            try {
                populateFromAuxiliaryData(programId);
                processedLines++;
            } catch (IndexAccessException e) {
                failedLines++;
                throw new RuntimeException("Error while trying to access auxiliary indexes for program: " + programId);
            } catch (Exception e) {
                failedLines++;
                LOG.error("Error while updating program " + programId + " with auxiliary data", e);
            }
        }
        
        DateTime endTime = now();
        
        return new RoviDataProcessingResult(processedLines, failedLines, startTime, endTime);
    }

    private void populateFromAuxiliaryData(String programId) throws IndexAccessException {
        Optional<Content> optionalResolved = resolveContent(programId);
        
        if (optionalResolved.isPresent()) {
            Content content = optionalResolved.get();
            populator.populateFromAuxiliaryOnly(content, programId);
            contentWriter.writeContent(content);
        }
    }
    
    private Optional<Content> resolveContent(String programId) {
        String canonicalUri = canonicalUriForProgram(programId);
        Maybe<Identified> maybeResolved = contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUri))
                .getFirstValue();
        
        if (maybeResolved.isNothing()) {
            LOG.error("Received an update for auxiliary data for a not existent program: " + programId);
            return Optional.absent();
        }
        
        return Optional.of((Content) maybeResolved.requireValue());
    }
    
    private DateTime now() {
        return DateTime.now(DateTimeZone.UTC);
    }
    
}
