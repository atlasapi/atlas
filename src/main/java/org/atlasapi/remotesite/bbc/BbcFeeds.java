package org.atlasapi.remotesite.bbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.remotesite.bbc.nitro.OffScheduleContentIngestTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BbcFeeds {

	public static final Logger log = LoggerFactory.getLogger(BbcFeeds.class);

	private static final String PID_PATTERN = "[a-z]\\d+[a-z0-9]+";
    
    private static final String SLASH_PROGRAMMES_BASE = "http://www.bbc.co.uk/programmes/";
    private static final String NITRO_BASE = "http://nitro.bbc.co.uk/programmes/";

    public static final Pattern SLASH_PROGRAMMES_URL_PATTERN = Pattern.compile(Pattern.quote(SLASH_PROGRAMMES_BASE) + PID_PATTERN);

	private static final Pattern PID_FINDER = Pattern.compile("/(" + PID_PATTERN + ")");
	
	public static String pidFrom(String uri) {
		Matcher matcher = PID_FINDER.matcher(uri);
		if (matcher.find()) {
			return matcher.group(1);
		}
		log.warn("could not extract a valid bbc pid from uri {}", uri);
		return null;
	}

	public static String slashProgrammesUriForPid(String pid) {
	    return slashProgrammesUri(SLASH_PROGRAMMES_BASE, pid);
	}

	public static String nitroUriForPid(String pid) {
	    return slashProgrammesUri(NITRO_BASE, pid);
	}

    private static String slashProgrammesUri(String base, String pid) {
        return base + pid;
    }
	

    public static boolean isACanonicalSlashProgrammesUri(String uri) {
        return SLASH_PROGRAMMES_URL_PATTERN.matcher(uri).matches();
    }
    
}
