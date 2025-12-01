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


import nl.knaw.dans.lib.util.ruleengine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

public class DatasetXmlValueCodesAreValidTest extends RuleTestFixture {
    private final Map<URI, Set<String>> supportedVocabs = Map.of(
        URI.create("https://vocab1.com"), Set.of("CODE.1A", "CODE.1B"),
        URI.create("https://vocab2.com"), Set.of("CODE.2A", "CODE.2B"),
        URI.create("https://vocab3.com"), Set.of("CODE.3A", "CODE.3B")
    );

    @Test
    public void should_return_ok_when_all_value_codes_are_valid() throws Exception {
        var xml = """
            <?xml version="1.0" encoding="UTF-8" standalone="no"?>
            <ddm:DDM xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/" xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/" xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/" xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/" xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" >
                <ddm:profile>
                    <dc:title>PAN-00008136 - knobbed sickle</dc:title>
                    <dcterms:description xml:lang="en">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>
                    <dcx-dai:creatorDetails>
                        <dcx-dai:organization>
                            <dcx-dai:name xml:lang="en">Portable Antiquities of the Netherlands</dcx-dai:name>
                            <dcx-dai:role>DataCurator</dcx-dai:role>
                        </dcx-dai:organization>
                    </dcx-dai:creatorDetails>
                    <ddm:created>2017-10-23T17:06:11+02:00</ddm:created>
                    <ddm:available>2017-10-23T17:06:11+02:00</ddm:available>
                    <ddm:audience>D37000</ddm:audience>
                    <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>
                </ddm:profile>
                <ddm:dcmiMetadata>
                    <ddm:subject subjectScheme="Vocab1" schemeURI="https://vocab1.com" valueCode="CODE.1A"/>
                    <ddm:subject subjectScheme="Vocab2" schemeURI="https://vocab2.com" valueCode="CODE.2B"/>
                    <ddm:subject subjectScheme="Vocab3" schemeURI="https://vocab3.com" valueCode="CODE.3A"/>
                </ddm:dcmiMetadata>
            </ddm:DDM>
            """;

        var document = parseXmlString(xml);
        var reader = spy(new XmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlValueCodesAreValid(reader, supportedVocabs).validate(Path.of("bagdir"));
        assertThat(result.getStatus()).isEqualTo(RuleResult.Status.SUCCESS);
    }

    @Test
    public void should_return_ERROR_when_one_term_used_not_found_in_vocab() throws Exception {
        var xml = """
            <?xml version="1.0" encoding="UTF-8" standalone="no"?>
            <ddm:DDM xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/" xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/" xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/" xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/" xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" >
                <ddm:profile>
                    <dc:title>PAN-00008136 - knobbed sickle</dc:title>
                    <dcterms:description xml:lang="en">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>
                    <dcx-dai:creatorDetails>
                        <dcx-dai:organization>
                            <dcx-dai:name xml:lang="en">Portable Antiquities of the Netherlands</dcx-dai:name>
                            <dcx-dai:role>DataCurator</dcx-dai:role>
                        </dcx-dai:organization>
                    </dcx-dai:creatorDetails>
                    <ddm:created>2017-10-23T17:06:11+02:00</ddm:created>
                    <ddm:available>2017-10-23T17:06:11+02:00</ddm:available>
                    <ddm:audience>D37000</ddm:audience>
                    <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>
                </ddm:profile>
                <ddm:dcmiMetadata>
                    <ddm:subject subjectScheme="Vocab1" schemeURI="https://vocab1.com" valueCode="INVALID"/>
                    <ddm:subject subjectScheme="Vocab2" schemeURI="https://vocab2.com" valueCode="CODE.2B"/>
                    <ddm:subject subjectScheme="Vocab3" schemeURI="https://vocab3.com" valueCode="CODE.3A"/>
                </ddm:dcmiMetadata>
            </ddm:DDM>
            """;

        var document = parseXmlString(xml);
        var reader = spy(new XmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlValueCodesAreValid(reader, supportedVocabs).validate(Path.of("bagdir"));
        assertThat(result.getStatus()).isEqualTo(RuleResult.Status.ERROR);
        assertThat(result.getErrorMessages()).contains("Invalid term for Vocab1: INVALID");
    }

    @Test
    public void should_return_ERROR_when_term_from_other_vocab_is_used() throws Exception {
        var xml = """
            <?xml version="1.0" encoding="UTF-8" standalone="no"?>
            <ddm:DDM xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/" xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/" xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/" xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/" xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" >
                <ddm:profile>
                    <dc:title>PAN-00008136 - knobbed sickle</dc:title>
                    <dcterms:description xml:lang="en">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>
                    <dcx-dai:creatorDetails>
                        <dcx-dai:organization>
                            <dcx-dai:name xml:lang="en">Portable Antiquities of the Netherlands</dcx-dai:name>
                            <dcx-dai:role>DataCurator</dcx-dai:role>
                        </dcx-dai:organization>
                    </dcx-dai:creatorDetails>
                    <ddm:created>2017-10-23T17:06:11+02:00</ddm:created>
                    <ddm:available>2017-10-23T17:06:11+02:00</ddm:available>
                    <ddm:audience>D37000</ddm:audience>
                    <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>
                </ddm:profile>
                <ddm:dcmiMetadata>
                    <ddm:subject subjectScheme="Vocab1" schemeURI="https://vocab1.com" valueCode="CODE.1A"/>
                    <ddm:subject subjectScheme="Vocab2" schemeURI="https://vocab2.com" valueCode="CODE.2B"/>
                    <ddm:subject subjectScheme="Vocab3" schemeURI="https://vocab3.com" valueCode="CODE.1A"/>
                </ddm:dcmiMetadata>
            </ddm:DDM>
            """;

        var document = parseXmlString(xml);
        var reader = spy(new XmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlValueCodesAreValid(reader, supportedVocabs).validate(Path.of("bagdir"));
        assertThat(result.getStatus()).isEqualTo(RuleResult.Status.ERROR);
        assertThat(result.getErrorMessages()).contains("Invalid term for Vocab3: CODE.1A");
    }

    @Test
    public void should_return_ERROR_and_multiple_message_when_more_than_one_term_is_wrong() throws Exception {
        var xml = """
            <?xml version="1.0" encoding="UTF-8" standalone="no"?>
            <ddm:DDM xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/" xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/" xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/" xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/" xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" >
                <ddm:profile>
                    <dc:title>PAN-00008136 - knobbed sickle</dc:title>
                    <dcterms:description xml:lang="en">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>
                    <dcx-dai:creatorDetails>
                        <dcx-dai:organization>
                            <dcx-dai:name xml:lang="en">Portable Antiquities of the Netherlands</dcx-dai:name>
                            <dcx-dai:role>DataCurator</dcx-dai:role>
                        </dcx-dai:organization>
                    </dcx-dai:creatorDetails>
                    <ddm:created>2017-10-23T17:06:11+02:00</ddm:created>
                    <ddm:available>2017-10-23T17:06:11+02:00</ddm:available>
                    <ddm:audience>D37000</ddm:audience>
                    <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>
                </ddm:profile>
                <ddm:dcmiMetadata>
                    <ddm:subject subjectScheme="Vocab1" schemeURI="https://vocab1.com" valueCode="INVALID1"/>
                    <ddm:subject subjectScheme="Vocab2" schemeURI="https://vocab2.com" valueCode="CODE.2B"/>
                    <ddm:subject subjectScheme="Vocab3" schemeURI="https://vocab3.com" valueCode="INVALID3"/>
                </ddm:dcmiMetadata>
            </ddm:DDM>
            """;

        var document = parseXmlString(xml);
        var reader = spy(new XmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlValueCodesAreValid(reader, supportedVocabs).validate(Path.of("bagdir"));
        assertThat(result.getStatus()).isEqualTo(RuleResult.Status.ERROR);
        assertThat(result.getErrorMessages()).contains("Invalid term for Vocab1: INVALID1", "Invalid term for Vocab3: INVALID3");
    }

    @Test
    public void should_return_SUCCESS_if_no_valueCode_present() throws Exception {
        var xml = """
            <?xml version="1.0" encoding="UTF-8" standalone="no"?>
            <ddm:DDM xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/" xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/" xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/" xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/" xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" >
                <ddm:profile>
                    <dc:title>PAN-00008136 - knobbed sickle</dc:title>
                    <dcterms:description xml:lang="en">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>
                    <dcx-dai:creatorDetails>
                        <dcx-dai:organization>
                            <dcx-dai:name xml:lang="en">Portable Antiquities of the Netherlands</dcx-dai:name>
                            <dcx-dai:role>DataCurator</dcx-dai:role>
                        </dcx-dai:organization>
                    </dcx-dai:creatorDetails>
                    <ddm:created>2017-10-23T17:06:11+02:00</ddm:created>
                    <ddm:available>2017-10-23T17:06:11+02:00</ddm:available>
                    <ddm:audience>D37000</ddm:audience>
                    <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>
                </ddm:profile>
                <ddm:dcmiMetadata>
                    <ddm:subject subjectScheme="Vocab1" schemeURI="https://vocab1.com" valueURI="https://someuri.com"/>
                    <ddm:subject subjectScheme="Vocab2" schemeURI="https://vocab2.com" valueURI="https://someuri.com"/>
                    <ddm:subject subjectScheme="Vocab3" schemeURI="https://vocab3.com" valueURI="https://someuri.com" />
                </ddm:dcmiMetadata>
            </ddm:DDM>
            """;
        var document = parseXmlString(xml);
        var reader = spy(new XmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlValueCodesAreValid(reader, supportedVocabs).validate(Path.of("bagdir"));
        assertThat(result.getStatus()).isEqualTo(RuleResult.Status.SUCCESS);
    }
}