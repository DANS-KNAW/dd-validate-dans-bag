package nl.knaw.dans.validatedansbag.core.rules;

import lombok.AllArgsConstructor;
import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;

import java.net.URI;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@AllArgsConstructor
public class DatasetXmlValueUrisAreValid implements BagValidatorRule {
    private final XmlReader xmlReader;
    private final Map<URI, Set<URI>> schemeUriToValidTermUris;

    @Override
    public RuleResult validate(Path path) throws Exception {
        var document = xmlReader.readXmlFile(path.resolve("metadata/dataset.xml"));
        List<String> allErrors = new LinkedList<>();

        for (var schemeUri : schemeUriToValidTermUris.keySet()) {
            var errors = xmlReader.xpathToStream(document, "/ddm:DDM/*/*[@schemeURI='" + schemeUri + "']")
                .map(node -> {
                    var valueUri = node.getAttributes().getNamedItem("valueURI").getTextContent();
                    var subjectScheme = node.getAttributes().getNamedItem("subjectScheme").getTextContent();

                    if (!schemeUriToValidTermUris.get(schemeUri).contains(URI.create(valueUri))) {
                        return String.format("Invalid term for %s: %s", subjectScheme, valueUri);
                    }
                    return null;
                }).filter(Objects::nonNull).toList();
            allErrors.addAll(errors);
        }

        if (allErrors.isEmpty())
            return RuleResult.ok();
        else
            return RuleResult.error(allErrors);
    }
}
