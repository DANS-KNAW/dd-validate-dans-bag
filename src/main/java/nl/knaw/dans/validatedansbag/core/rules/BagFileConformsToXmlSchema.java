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
import nl.knaw.dans.lib.util.XmlSchemaValidator;
import nl.knaw.dans.lib.util.ruleengine.BagValidatorRule;
import nl.knaw.dans.lib.util.ruleengine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class BagFileConformsToXmlSchema implements BagValidatorRule {
    protected final Path file;
    protected final FileService fileService;
    protected final String schema;
    private final XmlSchemaValidator xmlSchemaValidator;

    @Override
    public RuleResult validate(Path path) throws Exception {
        try {
            var fileName = path.resolve(file);
            log.debug("Validating {} against schema {}", fileName, schema);
            var errors = validateXmlFile(fileName, schema);

            if (!errors.isEmpty()) {
                var msg = String.format("%s does not conform to %s: \n%s",
                    file, schema, String.join("\n", errors));

                return RuleResult.error(msg);
            }
        }
        catch (SAXException e) {
            return RuleResult.error(e.getMessage(), e);
        }

        return RuleResult.ok();
    }

    private List<String> validateXmlFile(Path file, String schema) throws ParserConfigurationException, IOException, SAXException {
        var xml = fileService.readFileContents(file);
        var results = xmlSchemaValidator.validateDocument(new StreamSource(new ByteArrayInputStream(xml)), schema);

        return results.stream()
            .map(Throwable::getLocalizedMessage)
            .map(e -> String.format(" - %s", e))
            .collect(Collectors.toList());
    }

}
