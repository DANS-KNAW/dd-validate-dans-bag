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

import nl.knaw.dans.validatedansbag.core.service.*;
import nl.knaw.dans.validatedansbag.core.validator.*;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;

public class RuleTestFixture {
    protected final FileService fileService = Mockito.mock(FileService.class);
    protected final XmlReader xmlReader = Mockito.mock(XmlReader.class);
    protected final IdentifierValidator identifierValidator = new IdentifierValidatorImpl();
    protected final BagItMetadataReader bagItMetadataReader = Mockito.mock(BagItMetadataReader.class);
    protected final PolygonListValidator polygonListValidator = new PolygonListValidatorImpl();
    protected final OriginalFilepathsService originalFilepathsService = Mockito.mock(OriginalFilepathsService.class);
    protected final DataverseService dataverseService = Mockito.mock(DataverseService.class);

    protected final LicenseValidator licenseValidator = new LicenseValidatorImpl(dataverseService);
    protected final FilesXmlService filesXmlService = Mockito.mock(FilesXmlService.class);

    protected final OrganizationIdentifierPrefixValidator organizationIdentifierPrefixValidator = new OrganizationIdentifierPrefixValidatorImpl(
            List.of("USER1-", "U2:")
    );

    @AfterEach
    void afterEach() {
        Mockito.reset(fileService);
        Mockito.reset(xmlReader);
        Mockito.reset(bagItMetadataReader);
        Mockito.reset(dataverseService);
        Mockito.reset(originalFilepathsService);
    }

    protected Document parseXmlString(String str) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(str);
    }
}
