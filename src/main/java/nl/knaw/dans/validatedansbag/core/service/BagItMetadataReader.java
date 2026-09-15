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
package nl.knaw.dans.validatedansbag.core.service;

import nl.knaw.dans.bagit.domain.Bag;
import nl.knaw.dans.bagit.domain.Manifest;
import nl.knaw.dans.bagit.exceptions.CorruptChecksumException;
import nl.knaw.dans.bagit.exceptions.FileNotInPayloadDirectoryException;
import nl.knaw.dans.bagit.exceptions.InvalidBagitFileFormatException;
import nl.knaw.dans.bagit.exceptions.MaliciousPathException;
import nl.knaw.dans.bagit.exceptions.MissingBagitFileException;
import nl.knaw.dans.bagit.exceptions.MissingPayloadDirectoryException;
import nl.knaw.dans.bagit.exceptions.MissingPayloadManifestException;
import nl.knaw.dans.bagit.exceptions.UnparsableVersionException;
import nl.knaw.dans.bagit.exceptions.UnsupportedAlgorithmException;
import nl.knaw.dans.bagit.exceptions.VerificationException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface BagItMetadataReader {

    Optional<Bag> getBag(Path path);

    void verifyBag(Path path) throws MaliciousPathException, UnsupportedAlgorithmException, InvalidBagitFileFormatException, IOException, MissingPayloadManifestException,
        MissingPayloadDirectoryException, FileNotInPayloadDirectoryException, InterruptedException, MissingBagitFileException, CorruptChecksumException, VerificationException,
        UnparsableVersionException;

    List<String> getField(Path bagDir, String field);

    String getSingleField(Path bagDir, String field);

    Set<Manifest> getBagManifests(Bag bag);
}
