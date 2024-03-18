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

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurePathValidatorTest {

    @Test
    void isPathSecure_should_validate() {
        final Path basePath = Path.of("/this/is/base/test/path");
        final Path toTestPath = Path.of("/this/is/base/test/path/and/sub-path/sub-sub-path");
        SecurePathValidator securePathValidator = new SecurePathValidator(basePath);
        assertTrue(securePathValidator.IsPathSecure(toTestPath));
    }

    @Test
    void isPathSecure_should_not_validate_not_exist_path_1() {
        final Path basePath = Path.of("/this/is/base/test/path");
        final Path toTestPath = Path.of("/this/is/from-an-unknown-base/test/path/and/sub-path/sub-sub-path");
        SecurePathValidator securePathValidator = new SecurePathValidator(basePath);
        assertFalse(securePathValidator.IsPathSecure(toTestPath));
    }

    @Test
    void isPathSecure_should_not_validate_not_exist_path_2() {
        final Path basePath = Path.of("/this/is/base/test/path");
        final Path toTestPath = Path.of("sub-path/sub-sub-path");
        SecurePathValidator securePathValidator = new SecurePathValidator(basePath);
        assertFalse(securePathValidator.IsPathSecure(toTestPath));
    }

}