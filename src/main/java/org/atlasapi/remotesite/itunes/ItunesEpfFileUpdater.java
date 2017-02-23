package org.atlasapi.remotesite.itunes;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.s3.S3Client;

import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItunesEpfFileUpdater {

    private static final Logger log = LoggerFactory.getLogger(ItunesEpfFileUpdater.class);
    private static final ImmutableList<String> requiredPricingFiles = ImmutableList.of(
            "video_price"
    );
    private static final ImmutableList<String> requiredItunesFiles = ImmutableList.of(
            "application_detail",
            "artist",
            "artist_collection",
            "collection",
            "collection_video",
            "storefront",
            "video"
    );

    private final String username;
    private final String password;
    private final String feedPath;
    private final String localFilesPath;
    private final S3Client s3client;

    private String itunesDirectory;
    private String pricingDirectory;
    private File tempItunesDirectory;

    private ItunesEpfFileUpdater(
            String username,
            String password,
            String feedPath,
            String localFilesPath,
            S3Client s3Client
    ) {
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.feedPath = checkNotNull(feedPath);
        this.localFilesPath = checkNotNull(localFilesPath);
        this.s3client = checkNotNull(s3Client);
    }

    public static Builder builder() {
        return new Builder();
    }


    public void updateEpfFiles() throws Exception {
        Authenticator.setDefault(ItunesAuthenticator.create(username, password));

        createTempItunesDirectory();
        try {
            requiredItunesFiles.forEach(fileName -> {
                try {
                    saveFeedFile(itunesDirectory, fileName);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            });

            requiredPricingFiles.forEach(fileName -> {
                try {
                    saveFeedFile(pricingDirectory, fileName);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            });

        } catch (Exception e) {
            log.error("Error when trying to copy files from iTunes feed ", e);
        }

        deleteExistingItunesDirectory();

        tempItunesDirectory.renameTo(
                new File(String.format("%s%s", localFilesPath, itunesDirectory))
        );
    }

    private void deleteExistingItunesDirectory() throws IOException {
        File localDirectory = new File(localFilesPath);
        Optional<File> existingItunesDirectory = getExistingItunesDirectory(
                localDirectory.listFiles()
        );

        existingItunesDirectory.ifPresent(File::delete);
    }

    private void getLatestDirectoryNames() throws IOException {
        String inputLine;

        List<String> currentDirectories = Lists.newArrayList();

        Pattern itunesPattern = Pattern.compile("itunes\\d{8}");
        Pattern pricingPattern = Pattern.compile("pricing\\d{8}");

        URL currentUrl = new URL(feedPath);

        URLConnection urlConnection = currentUrl.openConnection();

        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(urlConnection.getInputStream())
        )) {
            while ((inputLine = in.readLine()) != null) {
                Matcher itunesMatcher = itunesPattern.matcher(inputLine);
                Matcher pricingMatcher = pricingPattern.matcher(inputLine);

                if (itunesMatcher.find()) {
                    currentDirectories.add(itunesMatcher.group(0));
                } else if (pricingMatcher.find()) {
                    currentDirectories.add(pricingMatcher.group(0));
                }
            }
        }

        itunesDirectory = currentDirectories.get(0);
        pricingDirectory = currentDirectories.get(1);
    }

    private Optional<File> getExistingItunesDirectory(File[] files) {
        File existingDir = null;

        for (File file : files) {
            if (file.isDirectory() &&
                    file.getName().startsWith("itunes") &&
                    file.getName().matches("itunes\\d{8}")) {
                existingDir = file;
            }
        }

        return Optional.ofNullable(existingDir);
    }

    private void createTempItunesDirectory() throws IOException {
        getLatestDirectoryNames();

        tempItunesDirectory = new File(
                String.format("%stmp_%s", localFilesPath, itunesDirectory)
        );

        tempItunesDirectory.mkdir();
    }

    private void saveFeedFile(String directory, String fileName) throws IOException {
        URL fileUrl = new URL(String.format("%s%s/%s.tbz", feedPath, directory, fileName));
        File itunesFeedFile = new File(String.format(
                "%stmp_%s/%s",
                localFilesPath,
                itunesDirectory,
                fileName
        ));

        FileUtils.copyURLToFile(
                fileUrl,
                itunesFeedFile
        );

        String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        s3client.put(String.format("%s_%s", fileName, currentDate), itunesFeedFile);
    }

    public static class ItunesAuthenticator extends Authenticator {

        private final String itunesUsername;
        private final String itunesPassword;

        private ItunesAuthenticator(String username, String password) {
            this.itunesUsername = username;
            this.itunesPassword = password;
        }

        public static ItunesAuthenticator create(String username, String password) {
            return new ItunesAuthenticator(username, password);
        }

        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(itunesUsername, itunesPassword.toCharArray());
        }
    }

    public static class Builder {

        private String username;
        private String password;
        private String feedPath;
        private String localFilesPath;
        private S3Client s3Client;

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withFeedPath(String feedPath) {
            this.feedPath = feedPath;
            return this;
        }

        public Builder withLocalFilesPath(String localFilesPath) {
            this.localFilesPath = localFilesPath;
            return this;
        }

        public Builder withS3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public ItunesEpfFileUpdater build() {
            return new ItunesEpfFileUpdater(username, password, feedPath, localFilesPath, s3Client);
        }
    }
}