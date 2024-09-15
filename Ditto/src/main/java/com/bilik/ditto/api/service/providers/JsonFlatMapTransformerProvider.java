package com.bilik.ditto.api.service.providers;

import com.bilik.proto.video.ad.VideoAd;
import com.bilik.ditto.transformation.NestedDataToFlatMapTransformation;
import com.bilik.ditto.core.configuration.ResourceUtils;
import com.bilik.ditto.core.exception.InvalidUserInputException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In case this non-spring solution won't work within JAR, try to use PathMatchingResourcePatternResolver
 */
public class JsonFlatMapTransformerProvider {

    private static final Logger log = LoggerFactory.getLogger(JsonFlatMapTransformerProvider.class);

    private static final String DIR = "JsonFlatMaps/";
    private static final String FILE_EXTENSION = ".json";

    /**
     * Holds pairs <User friendly Name, FileName>
     */
    private static final Map<String, String> availableTransformations;

    /*
     * This way we can validate them before using
     */
    static {
        try {
            List<String> files = listFiles(DIR);
            log.info("Available JsonFlatMapperTransformations are : {}", files);
            availableTransformations = files.stream()
                    .collect(Collectors.toUnmodifiableMap(
                            file -> file.substring(0, file.lastIndexOf('.')),
                            file -> file
                    ));

            // this part is for validation, if supplied json's are correct
            for (String file : files) {
                String json = getResourceFileAsString(file);
                try {
                    var configMap = NestedDataToFlatMapTransformation.buildMapFromString(json);
                    var transformation = new NestedDataToFlatMapTransformation(Video.VideoAd.getDescriptor(), configMap);
                    log.info("{} has been validated", file);
                } catch (Exception ex) {
                    throw new InvalidUserInputException("NestedDataToFlatMapTransformation could not be instantiated from file " + file, ex);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Set<String> getAvailableFlatMapTransformations() {
        return availableTransformations.keySet();
    }

    public static Optional<String> getFlatMapTransformationJson(String name) {
        String fileName = availableTransformations.get(name);
        if (fileName == null) {
            return Optional.empty();
        }
        return Optional.of(getResourceFileAsString(fileName));
    }

    private static String getResourceFileAsString(String fileName) {
        String fullPath = DIR + fileName;
        InputStream is = getResourceAsStream(fullPath);
        if (is != null) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                log.info("Loaded JsonFlatMapperTransformation: {}", fullPath);
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("JsonFlatMapperTransformation with name " + fullPath + " could not be found!");
        }
    }

    private static List<String> listFiles(String path) throws IOException {
        return ResourceUtils.listClassPathResources(path)
                .stream()
                .map(Resource::getFilename)
                .filter(file -> file.endsWith(FILE_EXTENSION))
                .toList();
    }

    private static InputStream getResourceAsStream(String resource) {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);

        return in != null ? in :
                JsonFlatMapTransformerProvider.class.getResourceAsStream(resource);
    }

}
