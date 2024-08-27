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
public class DatasetXmlValueCodesAreValid implements BagValidatorRule {
    private final XmlReader xmlReader;
    private final Map<URI, Set<String>> schemeUriToValidTermCodes;

    @Override
    public RuleResult validate(Path path) throws Exception {
        var document = xmlReader.readXmlFile(path.resolve("metadata/dataset.xml"));
        List<String> allErrors = new LinkedList<>();

        for (var schemeUri : schemeUriToValidTermCodes.keySet()) {
            var errors = xmlReader.xpathToStream(document, "/ddm:DDM/*/*[@schemeURI='" + schemeUri + "']")
                .map(node -> {
                    var valueCodeAttr = node.getAttributes().getNamedItem("valueCode");
                    if (valueCodeAttr != null) {
                        var valueCode = valueCodeAttr.getTextContent();
                        var subjectScheme = node.getAttributes().getNamedItem("subjectScheme").getTextContent();

                        if (!schemeUriToValidTermCodes.get(schemeUri).contains(valueCode)) {
                            return String.format("Invalid term for %s: %s", subjectScheme, valueCode);
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
}
