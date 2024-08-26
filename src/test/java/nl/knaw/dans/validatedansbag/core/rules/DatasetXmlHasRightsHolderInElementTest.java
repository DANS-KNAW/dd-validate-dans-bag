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

public class DatasetXmlHasRightsHolderInElementTest extends RuleTestFixture {

    @Test
    void should_return_SUCCESS_if_rightsHolder_element_present() throws Exception {
        var xml = """
            <ddm:DDM xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:dct="http://purl.org/dc/terms/"
                     xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/">
                <ddm:profile>
                    <dcx-dai:creatorDetails>
                        <dcx-dai:author>
                            <dcx-dai:role>Distributor</dcx-dai:role>
                        </dcx-dai:author>
                    </dcx-dai:creatorDetails>
                    <ddm:accessRights>OPEN_ACCESS_FOR_REGISTERED_USERS</ddm:accessRights>
                </ddm:profile>
                <ddm:dcmiMetadata>
                    <dct:license xsi:type="dct:URI">http://creativecommons.org/licenses/by-sa/4.0</dct:license>
                    <dct:rightsHolder>Johnny</dct:rightsHolder>
                </ddm:dcmiMetadata>
            </ddm:DDM>
            """;

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlHasRightsHolderInElement(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void should_return_ERROR_if_rightsHolder_element_not_present() throws Exception {
        var xml = """
            <ddm:DDM xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:dct="http://purl.org/dc/terms/"
                     xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/">
                <ddm:profile>
                    <dcx-dai:creatorDetails>
                        <dcx-dai:author>
                            <dcx-dai:role>Distributor</dcx-dai:role>
                        </dcx-dai:author>
                    </dcx-dai:creatorDetails>
                    <ddm:accessRights>OPEN_ACCESS_FOR_REGISTERED_USERS</ddm:accessRights>
                </ddm:profile>
                <ddm:dcmiMetadata>
                    <dct:license xsi:type="dct:URI">http://creativecommons.org/licenses/by-sa/4.0</dct:license>
                </ddm:dcmiMetadata>
            </ddm:DDM>
            """;

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlHasRightsHolderInElement(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

}
