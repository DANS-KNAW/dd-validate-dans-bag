/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.validatedansbag.core.rules;

import lombok.AllArgsConstructor;
import nl.knaw.dans.lib.util.ruleengine.BagValidatorRule;
import nl.knaw.dans.lib.util.ruleengine.RuleResult;
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
    private static final String ABR_OLD_BASE_URL = "https://data.cultureelerfgoed.nl/term/id/rn/";
    private static final String ABR_NEW_BASE_URL = "https://data.cultureelerfgoed.nl/term/id/abr/";

    private final XmlReader xmlReader;
    private final Map<URI, Set<URI>> schemeUriToValidTermUris;

    @Override
    public RuleResult validate(Path path) throws Exception {
        var document = xmlReader.readXmlFile(path.resolve("metadata/dataset.xml"));
        List<String> allErrors = new LinkedList<>();

        for (var schemeUri : schemeUriToValidTermUris.keySet()) {
            var errors = xmlReader.xpathToStream(document, "/ddm:DDM/*/*[@schemeURI='" + schemeUri + "']")
                .map(node -> {
                    var valueUriAttr = node.getAttributes().getNamedItem("valueURI");

                    if (valueUriAttr != null) {
                        var valueUri = convertOldAbrToNew(valueUriAttr.getTextContent());
                        var subjectScheme = node.getAttributes().getNamedItem("subjectScheme").getTextContent();

                        if (!schemeUriToValidTermUris.get(schemeUri).contains(URI.create(valueUri))) {
                            return String.format("Invalid term for %s: %s", subjectScheme, valueUri);
                        }
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

    private String convertOldAbrToNew(String uri) {
        if (uri.startsWith(ABR_OLD_BASE_URL)) {
            return uri.replace(ABR_OLD_BASE_URL, ABR_NEW_BASE_URL);
        }
        return uri;
    }
}
