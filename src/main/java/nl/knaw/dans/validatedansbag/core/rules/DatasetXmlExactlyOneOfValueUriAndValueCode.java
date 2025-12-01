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
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@AllArgsConstructor
@Slf4j
public class DatasetXmlExactlyOneOfValueUriAndValueCode implements BagValidatorRule {
    public final static String SCHEME_URI_ABR_OLD = "https://data.cultureelerfgoed.nl/term/id/rn/a4a7933c-e096-4bcf-a921-4f70a78749fe";
    public final static String SCHEME_URI_ABR_PLUS = "https://data.cultureelerfgoed.nl/term/id/abr/b6df7840-67bf-48bd-aa56-7ee39435d2ed";
    public final static String SCHEME_URI_ABR_COMPLEX = "https://data.cultureelerfgoed.nl/term/id/abr/e9546020-4b28-4819-b0c2-29e7c864c5c0";
    public final static String SCHEME_URI_ABR_ARTIFACT = "https://data.cultureelerfgoed.nl/term/id/abr/22cbb070-6542-48f0-8afe-7d98d398cc0b";
    public final static String SCHEME_URI_ABR_PERIOD = "https://data.cultureelerfgoed.nl/term/id/abr/9b688754-1315-484b-9c89-8817e87c1e84";
    public final static String SCHEME_URI_ABR_RAPPORT_TYPE = "https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e";
    public final static String SCHEME_URI_ABR_VERWERVINGSWIJZE = "https://data.cultureelerfgoed.nl/term/id/abr/554ca1ec-3ed8-42d3-ae4b-47bcb848b238";

    private final XmlReader xmlReader;

    @Override
    public RuleResult validate(Path path) throws Exception {
        var document = xmlReader.readXmlFile(path.resolve("metadata/dataset.xml"));

        var abrNodes = xmlReader.xpathsToStream(document,
            List.of("/ddm:DDM/ddm:dcmiMetadata/ddm:subject[@schemeURI='" + SCHEME_URI_ABR_OLD + "']",
                "/ddm:DDM/ddm:dcmiMetadata/ddm:subject[@schemeURI='" + SCHEME_URI_ABR_PLUS + "']",
                "/ddm:DDM/ddm:dcmiMetadata/ddm:subject[@schemeURI='" + SCHEME_URI_ABR_COMPLEX + "']",
                "/ddm:DDM/ddm:dcmiMetadata/ddm:subject[@schemeURI='" + SCHEME_URI_ABR_ARTIFACT + "']",
                "/ddm:DDM/ddm:dcmiMetadata/ddm:reportNumber[@schemeURI='" + SCHEME_URI_ABR_RAPPORT_TYPE + "']",
                "/ddm:DDM/ddm:dcmiMetadata/ddm:acquisitionMethod[@schemeURI='" + SCHEME_URI_ABR_VERWERVINGSWIJZE + "']",
                "/ddm:DDM/ddm:dcmiMetadata/ddm:temporal[@schemeURI='" + SCHEME_URI_ABR_PERIOD + "']"));

        var errors = Stream.of(abrNodes)
            .flatMap(node -> node)
            .map(value -> {
                log.debug("Validating ABR element: {}", value.getLocalName());

                var valueUri = value.getAttributes().getNamedItem("valueURI");
                var valueCode = value.getAttributes().getNamedItem("valueCode");

                if (valueUri == null && valueCode == null) {
                    return "Element " + value.getLocalName() + " has neither valueURI nor valueCode";
                }
                else if (valueUri != null && valueCode != null) {
                    return "Element " + value.getLocalName() + " has both valueURI and valueCode";
                }
                return null; // no error: OK
            }).filter(Objects::nonNull).toList();

        if (errors.isEmpty())
            return RuleResult.ok();
        else
            return RuleResult.error(errors);
    }
}
