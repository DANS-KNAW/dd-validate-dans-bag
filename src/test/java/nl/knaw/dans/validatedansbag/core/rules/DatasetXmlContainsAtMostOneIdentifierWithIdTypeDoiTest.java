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

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatasetXmlContainsAtMostOneIdentifierWithIdTypeDoiTest extends RuleTestFixture {

    @Test
    void should_return_SKIP_DEPENDENCIES_when_no_dois_present() throws Exception {
        final String xml = """
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:dcmiMetadata>
                </ddm:dcmiMetadata>
            </ddm:DDM>""";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlContainsAtMostOneIdentifierWithIdTypeDoi(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SKIP_DEPENDENCIES, result.getStatus());
    }

    @Test
    void should_return_SUCCESS_when_one_dois_present() throws Exception {
        final String xml = """
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:dcmiMetadata>
                    <dcterms:identifier xsi:type="id-type:DOI">10.1234/fantasy-doi-id</dcterms:identifier>
                </ddm:dcmiMetadata>
            </ddm:DDM>""";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlContainsAtMostOneIdentifierWithIdTypeDoi(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void should_return_ERROR_when_two_dois_present() throws Exception {
        final String xml = """
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:dcmiMetadata>
                    <dcterms:identifier xsi:type="id-type:DOI">10.1234/fantasy-doi-id</dcterms:identifier>
                    <dcterms:identifier xsi:type="id-type:DOI">10.1234/fantasy-doi-id2</dcterms:identifier>
                </ddm:dcmiMetadata>
            </ddm:DDM>""";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlContainsAtMostOneIdentifierWithIdTypeDoi(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }
}
