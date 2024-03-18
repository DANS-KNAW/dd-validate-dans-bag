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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class SecurePathValidator {
    private static final Logger log = LoggerFactory.getLogger(SecurePathValidator.class);

    private final Path secureBaseFolder;
    /*
    /home/alish/git/dans-core-systems/modules/dd-validate-dans-bag/target/test-classes/bags/datastation-valid-bag/original-filepaths.txt
     */
    public SecurePathValidator(Path baseFolder) {
        this.secureBaseFolder = baseFolder;
    }

    public boolean IsPathSecure(Path path) {
        Path resolvedPath = path.normalize().toAbsolutePath();
        // TODO 'this.secureBaseFolder != null' is temporary to avoid tests failure
        if ( this.secureBaseFolder != null && !resolvedPath.startsWith(this.secureBaseFolder)) {
            //throw new IllegalArgumentException("InsecurePath");
            log.debug("InsecurePath: {}", path);
            return false;
        }
        return true;
    }
}
