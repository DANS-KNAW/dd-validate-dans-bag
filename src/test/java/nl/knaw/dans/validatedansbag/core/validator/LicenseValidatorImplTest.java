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
package nl.knaw.dans.validatedansbag.core.validator;

import nl.knaw.dans.validatedansbag.core.validator.LicenseValidatorImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LicenseValidatorImplTest {

    @Test
    void isValidLicense() {
        var license = "http://creativecommons.org/licenses/by-nc-nd/4.0/";
        assertTrue(new LicenseValidatorImpl().isValidLicense(license));
    }

    @Test
    void isValidLicenseWhenTrailingSlashIsMissing() {
        var license = "http://creativecommons.org/licenses/by-nc-nd/4.0";
        assertTrue(new LicenseValidatorImpl().isValidLicense(license));
    }
    @Test
    void isInvalidLicense() {
        var license = "http://creativecommons.org/licenses/by-nc-nd/4";
        assertFalse(new LicenseValidatorImpl().isValidLicense(license));
        assertFalse(new LicenseValidatorImpl().isValidLicense("something completely different"));
    }
}