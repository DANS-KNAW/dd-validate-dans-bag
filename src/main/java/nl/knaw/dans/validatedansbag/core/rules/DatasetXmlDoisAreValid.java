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
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ruleengine.BagValidatorRule;
import nl.knaw.dans.lib.util.ruleengine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;

import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class DatasetXmlDoisAreValid implements BagValidatorRule {
    private static final Pattern doiPattern = Pattern.compile("^10(\\.\\d+)+/.+");

    private final XmlReader xmlReader;

    @Override
    public RuleResult validate(Path path) throws Exception {
        var document = xmlReader.readXmlFile(path.resolve("metadata/dataset.xml"));
        var idTypePrefix = document.lookupPrefix(XmlReader.NAMESPACE_ID_TYPE);
        var expr = String.format("/ddm:DDM/ddm:dcmiMetadata/dcterms:identifier[@xsi:type=\"%s:DOI\"]", idTypePrefix);
        var nodes = xmlReader.xpathToStreamOfStrings(document, expr);
        var invalidDois = nodes
                .peek(node -> log.debug("Validating if {} matches pattern {}", node, doiPattern))
                .filter((text) -> !doiPattern.matcher(text).matches())
                .collect(Collectors.joining(", "));

        log.debug("Identifiers (DOI) that do not match the pattern: {}", invalidDois);

        if (!invalidDois.isEmpty()) {
            return RuleResult.error(String.format(
                    "dataset.xml: Invalid DOIs: %s", invalidDois
            ));
        }

        return RuleResult.ok();
    }
}
