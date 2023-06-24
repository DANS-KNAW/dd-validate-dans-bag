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

import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArchisIdentifiersHaveAtMost10CharactersTest extends RuleTestFixture {
    @Test
    void should_return_SUCCESS_if_all_ok() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">id1</dct:identifier>\n"
                + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">id2</dct:identifier>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new ArchisIdentifiersHaveAtMost10Characters(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void should_return_ERROR_if_some_too_long() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">niet kunnen vinden1</dct:identifier>\n"
                + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">niet kunnen vinden2</dct:identifier>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new ArchisIdentifiersHaveAtMost10Characters(reader).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
        assertEquals(2, result.getErrorMessages().size());
        assertTrue(result.getErrorMessages().get(0).contains("niet kunnen vinden1"));
        assertTrue(result.getErrorMessages().get(1).contains("niet kunnen vinden2"));
    }

}
