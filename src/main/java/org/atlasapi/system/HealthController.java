package org.atlasapi.system;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HealthController {

	@RequestMapping("/system/threads")
	public void showThreads(HttpServletResponse response) throws IOException {
		response.setContentType("text/html");
		response.setStatus(200);
		ServletOutputStream out = response.getOutputStream();
		out.print("<html><body>");
		final Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
		for (Entry<Thread, StackTraceElement[]> entry : traces.entrySet()) {
			out.print(String.format("<font color='blue'>Stack trace for thread '%s'</font>:<br/>", entry.getKey().getName()));
			for (StackTraceElement e : entry.getValue()) {
				out.print(e.toString() + "<br/>");
			}
			out.print("<hr/>");
		}
		out.print("</body></html>");
		out.close();
	}

	@RequestMapping("/system/alive")
    public void isAlive(HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        response.setStatus(200);
    }
}
