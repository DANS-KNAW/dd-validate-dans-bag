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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Map;

import static nl.knaw.dans.validatedansbag.core.rules.DatasetXmlExactlyOneOfValueUriAndValueCode.SCHEME_URI_ABR_ARTIFACT;
import static nl.knaw.dans.validatedansbag.core.rules.DatasetXmlExactlyOneOfValueUriAndValueCode.SCHEME_URI_ABR_COMPLEX;
import static nl.knaw.dans.validatedansbag.core.rules.DatasetXmlExactlyOneOfValueUriAndValueCode.SCHEME_URI_ABR_OLD;
import static nl.knaw.dans.validatedansbag.core.rules.DatasetXmlExactlyOneOfValueUriAndValueCode.SCHEME_URI_ABR_PERIOD;
import static nl.knaw.dans.validatedansbag.core.rules.DatasetXmlExactlyOneOfValueUriAndValueCode.SCHEME_URI_ABR_RAPPORT_TYPE;
import static nl.knaw.dans.validatedansbag.core.rules.DatasetXmlExactlyOneOfValueUriAndValueCode.SCHEME_URI_ABR_VERWERVINGSWIJZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

@Slf4j
public class DatasetXmlExactlyOneOfValueUriAndValueCodeTest extends RuleTestFixture {
    private static final Map<String, String> schemeUriToElementName = Map.of(
        SCHEME_URI_ABR_RAPPORT_TYPE, "reportNumber",
        SCHEME_URI_ABR_VERWERVINGSWIJZE, "acquisitionMethod",
        SCHEME_URI_ABR_OLD, "subject",
        SCHEME_URI_ABR_COMPLEX, "subject",
        SCHEME_URI_ABR_ARTIFACT, "subject",
        SCHEME_URI_ABR_PERIOD, "temporal"
    );

    @Test
    public void should_return_SUCCESS_when_only_valueURI_present() throws Exception {
        for (var schemeElement : schemeUriToElementName.entrySet()) {
            log.debug("Testing schemeURI: {}", schemeElement.getKey());
            var xml = String.format("""
                <ddm:DDM
                        xmlns:dc="http://purl.org/dc/elements/1.1/"
                        xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                        xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                        xmlns:dcterms="http://purl.org/dc/terms/"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                    <ddm:dcmiMetadata>
                        <ddm:subject schemeURI="%s" valueURI="%s"/>
                    </ddm:dcmiMetadata>
                </ddm:DDM>""", schemeElement.getKey(), "http://dummy.com");

            var document = parseXmlString(xml);
            var reader = spy(new XmlReaderImpl());
            Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

            var result = new DatasetXmlExactlyOneOfValueUriAndValueCode(reader).validate(Path.of("bagdir"));

            assertThat(result.getStatus()).isEqualTo(RuleResult.Status.SUCCESS);
        }
    }

    @Test
    public void should_return_ERROR_when_both_valueURI_and_valueCode_present() throws Exception {
        for (var schemeElement : schemeUriToElementName.entrySet()) {
            log.debug("Testing schemeURI: {}", schemeElement.getKey());
            var xml = String.format("""
                <ddm:DDM
                        xmlns:dc="http://purl.org/dc/elements/1.1/"
                        xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                        xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                        xmlns:dcterms="http://purl.org/dc/terms/"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                    <ddm:dcmiMetadata>
                        <ddm:%s schemeURI="%s" valueURI="%s" valueCode="%s"/>
                    </ddm:dcmiMetadata>
                </ddm:DDM>""", schemeElement.getValue(), schemeElement.getKey(), "http://dummy.com", "DUMMY.CODE");

            var document = parseXmlString(xml);
            var reader = spy(new XmlReaderImpl());
            Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

            var result = new DatasetXmlExactlyOneOfValueUriAndValueCode(reader).validate(Path.of("bagdir"));

            assertThat(result.getStatus()).isEqualTo(RuleResult.Status.ERROR);
            assertThat(result.getErrorMessages()).contains(String.format("Element %s has both valueURI and valueCode", schemeElement.getValue()));
        }
    }

    @Test
    public void should_return_ERROR_when_neither_valueURI_nor_valueCode_present() throws Exception {
        for (var schemeElement : schemeUriToElementName.entrySet()) {
            log.debug("Testing schemeURI: {}", schemeElement.getKey());
            var xml = String.format("""
                <ddm:DDM
                        xmlns:dc="http://purl.org/dc/elements/1.1/"
                        xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                        xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                        xmlns:dcterms="http://purl.org/dc/terms/"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                    <ddm:dcmiMetadata>
                        <ddm:%s schemeURI="%s"/>
                    </ddm:dcmiMetadata>
                </ddm:DDM>""", schemeElement.getValue(), schemeElement.getKey());

            var document = parseXmlString(xml);
            var reader = spy(new XmlReaderImpl());
            Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

            var result = new DatasetXmlExactlyOneOfValueUriAndValueCode(reader).validate(Path.of("bagdir"));

            assertThat(result.getStatus()).isEqualTo(RuleResult.Status.ERROR);
            assertThat(result.getErrorMessages()).contains(String.format("Element %s has neither valueURI nor valueCode", schemeElement.getValue()));
        }
    }

}
