package org.atlasapi.templates;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.common.media.MimeType;

import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.ServletContextAware;

@Controller
public class HandlebarsTemplateRenderer implements ServletContextAware {

    private File templateDir;
    private String prefix;
    private Map<String, Set<String>> paramNames = Maps.newHashMap();

    @Override
    public void setServletContext(ServletContext context) {
        setTemplateDir(context.getRealPath(prefix));
    }

    private void setTemplateDir(String templateDirPath) {
        this.templateDir = new File(templateDirPath);
        if (!templateDir.exists()) {
            throw new IllegalStateException("Template Directory does not exist: "
                    + templateDir.getAbsolutePath());
        }
        compileTemplates(templateDir);
    }

    private void compileTemplates(File dir) {
        // TODO : Using Handlebars get templates from WEB-INF/templates dir.
//        Set<File> soyfiles = soyfilesFrom(dir);
//        if (soyfiles.isEmpty()) {
//            // no files in dir, nothing to do
//            this.paramNames = Maps.newHashMap();
//            return;
//        }
//
//        Map<String, Set<String>> newParams = Maps.newHashMap();
//
//        SoyFileSet.Builder builder = builder();
//        builder.setCompileTimeGlobals(constants);
//
//        for (File file : soyfiles) {
//            builder.add(file);
//            extractParams(newParams, file);
//        }
//        this.soyFiles = builder.build().compileToJavaObj();
//        this.paramNames = newParams;
    }

    @RequestMapping("/system/handlebars/recompile")
    public void compileTemplates(HttpServletResponse response) throws IOException {
        StringBuilder b = new StringBuilder();
        try {
            compileTemplates(templateDir);
            b.append("Success : templates compiled").append('\n');
            // Getting param names via Handlebar....
            for (Map.Entry<String, Set<String>> entry : paramNames.entrySet()) {
                b.append("Template name: " + entry.getKey() + " params: " + entry.getValue())
                        .append('\n');
            }
        } catch (Exception e) {
            b.append("Error: ").append(e.getMessage()).append('\n');
            for (StackTraceElement traceLine : e.getStackTrace()) {
                b.append(traceLine).append('\n');
            }
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(MimeType.TEXT_PLAIN.toString());
        response.getOutputStream().print(b.toString());
    }

    @Required
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    private void extractParams(Map<String, Set<String>> paramNames, File file) {
        // TODO : Extracting parameters.
//        try {
//            SoyFileNode soyFile = parseSoyFile(file);
//            String namespace = soyFile.getNamespace();
//            for (TemplateNode template : soyFile.getChildren()) {
//                Set<String> params = Sets.newHashSet();
//                if (template.getSoyDocParams() != null) {
//                    for (TemplateNode.SoyDocParam param : template.getSoyDocParams()) {
//                        params.add(param.key);
//                    }
//                }
//                String name = namespace + template.getPartialTemplateName();
//                paramNames.put(name, params);
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }
}
