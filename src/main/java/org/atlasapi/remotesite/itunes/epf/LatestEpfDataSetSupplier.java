package org.atlasapi.remotesite.itunes.epf;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileFilter;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

public class LatestEpfDataSetSupplier implements Supplier<EpfDataSet> {

    private File dataDirectory;

    public LatestEpfDataSetSupplier(File dataDirectory) {
        checkArgument(checkNotNull(dataDirectory).isDirectory());
        this.dataDirectory = dataDirectory;
    }
    
    @Override
    public EpfDataSet get() {
        File latestDirectory = latestDirectory();
        if (latestDirectory != null) {
            return new EpfDataSet(latestDirectory);
        }
        throw new IllegalStateException("Couldn't find EPF data set");
    }

    private File latestDirectory() {
        return Ordering.natural().max(ImmutableList.copyOf(
                dataDirectory.listFiles(pathname ->
                        pathname.isDirectory() &&
                                pathname.getName().startsWith("itunes") &&
                                pathname.getName().matches("itunes\\d{8}")
                )
        ));
    }

}
