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
import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidator;

import java.nio.file.Path;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class DatasetXmlDaisAreValid implements BagValidatorRule {
    private final XmlReader xmlReader;
    private final IdentifierValidator identifierValidator;

    @Override
    public RuleResult validate(Path path) throws Exception {
        var document = xmlReader.readXmlFile(path.resolve("metadata/dataset.xml"));
        var expr = "//dcx-dai:DAI";
        var invalidDais = xmlReader.xpathToStreamOfStrings(document, expr)
                .peek(dai -> log.debug("Validating if {} is a valid DAI", dai))
                .filter((dai) -> !identifierValidator.validateDai(dai))
                .collect(Collectors.toList());

        log.debug("Identifiers (DAI) that do not match the pattern: {}", invalidDais);

        if (!invalidDais.isEmpty()) {
            var message = String.join(", ", invalidDais);
            return RuleResult.error("dataset.xml: Invalid DAIs: " + message);
        }

        return RuleResult.ok();
    }
}
