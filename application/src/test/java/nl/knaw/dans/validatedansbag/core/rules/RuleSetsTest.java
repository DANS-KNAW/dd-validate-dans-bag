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

import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.validatedansbag.core.engine.RuleEngineImpl;
import nl.knaw.dans.validatedansbag.core.service.*;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.LicenseValidator;
import nl.knaw.dans.validatedansbag.core.validator.OrganizationIdentifierPrefixValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.PolygonListValidatorImpl;
import nl.knaw.dans.validatedansbag.resources.ValidateResource;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuleSetsTest {
    private static final DataverseService dataverseService = Mockito.mock(DataverseService.class);
    private static final XmlSchemaValidator xmlSchemaValidator = Mockito.mock(XmlSchemaValidator.class);

    private static final LicenseValidator licenseValidator = new LicenseValidator() {

        @Override
        public boolean isValidUri(String license) {
            return true;
        }

        @Override
        public boolean isValidLicense(String license) throws IOException, DataverseException {
            return true;
        }
    };


    @Test
    public void dataStationsRuleSet_should_be_consistent() throws Exception {
        var fileService = new FileServiceImpl();
        var bagItMetadataReader = new BagItMetadataReaderImpl();
        var xmlReader = new XmlReaderImpl();
        var polygonListValidator = new PolygonListValidatorImpl();
        var originalFilepathsService = new OriginalFilepathsServiceImpl(fileService);
        var filesXmlService = new FilesXmlServiceImpl(xmlReader);
        var identifierValidator = new IdentifierValidatorImpl();

        var organizationIdentifierPrefixValidator = new OrganizationIdentifierPrefixValidatorImpl(
                List.of("u1:", "u2:")
        );

        // set up the engine and the service that has a default set of rules
        var ruleEngine = new RuleEngineImpl();
        var ruleSets = new RuleSets(
                dataverseService, fileService, filesXmlService, originalFilepathsService, xmlReader,
                bagItMetadataReader, xmlSchemaValidator, licenseValidator, identifierValidator, polygonListValidator, organizationIdentifierPrefixValidator
        );

        new RuleEngineServiceImpl(ruleEngine, fileService, ruleSets.getDataStationSet());
        assertTrue(true); // if we get here, the rule set is consistent
    }

    @Test
    public void vaasRuleSet_should_be_consistent() throws Exception {
        var fileService = new FileServiceImpl();
        var bagItMetadataReader = new BagItMetadataReaderImpl();
        var xmlReader = new XmlReaderImpl();
        var polygonListValidator = new PolygonListValidatorImpl();
        var originalFilepathsService = new OriginalFilepathsServiceImpl(fileService);
        var filesXmlService = new FilesXmlServiceImpl(xmlReader);
        var identifierValidator = new IdentifierValidatorImpl();

        var organizationIdentifierPrefixValidator = new OrganizationIdentifierPrefixValidatorImpl(
                List.of("u1:", "u2:")
        );

        // set up the engine and the service that has a default set of rules
        var ruleEngine = new RuleEngineImpl();
        var ruleSets = new RuleSets(
                dataverseService, fileService, filesXmlService, originalFilepathsService, xmlReader,
                bagItMetadataReader, xmlSchemaValidator, licenseValidator, identifierValidator, polygonListValidator, organizationIdentifierPrefixValidator
        );

        new RuleEngineServiceImpl(ruleEngine, fileService, ruleSets.getVaasSet());
        assertTrue(true); // if we get here, the rule set is consistent
    }

}
