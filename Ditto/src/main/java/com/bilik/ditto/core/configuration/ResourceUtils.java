package com.bilik.ditto.core.configuration;

import com.bilik.ditto.core.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ResourceUtils {

    private static final Logger log = LoggerFactory.getLogger(ResourceUtils.class);

    public static List<ClassPathResource> listClassPathResources(String classPathDir) throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String resourceLocation = "classpath*:" + classPathDir + "*";
        return Arrays.stream(resolver.getResources(resourceLocation))
                .map(resource -> new ClassPathResource(classPathDir + resource.getFilename()))
                .toList();
    }

    public static Map<String, String> loadPropertiesAsMap(Resource resource) throws IOException {
        return convertPropertiesToMap(loadProperties(resource));
    }

    public static Properties loadProperties(Resource resource) throws IOException {
        Properties props = new Properties();
        try (InputStream is = resource.getInputStream()) {
            props.load(is);
        }
        return filter(props);
    }

    public static Map<String, String> convertPropertiesToMap(Properties properties) {
        Map<String, String> propertiesMap = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            propertiesMap.put(key, properties.getProperty(key));
        }
        return propertiesMap;
    }

    /**
     * ConfigGenerator creates properties like:"security.protocol" = "SSL", which also takes quote as
     * part of key and value. So after loading them, we will replace all quotes with nothing
     *<p>Second is filtering empty properties with either empty key or value
     */
    private static Properties filter(Properties properties) {
        Properties returnProps = new Properties();
        for (String key : properties.stringPropertyNames()) {
            String newKey = key.replaceAll("\"", "");
            String newValue = properties.getProperty(key).replaceAll("\"", "");
            if (StringUtils.isNotBlank(newValue) && StringUtils.isNotBlank(newKey)) {
                returnProps.setProperty(newKey, newValue);
            } else {
                log.debug("Found invalid property [Key={}, value={}]", key, newValue);
            }
        }
        return returnProps;
    }

}
