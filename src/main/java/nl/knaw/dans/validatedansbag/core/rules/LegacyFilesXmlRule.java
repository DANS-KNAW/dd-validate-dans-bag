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

import lombok.AllArgsConstructor;
import nl.knaw.dans.lib.util.ruleengine.BagValidatorRule;
import nl.knaw.dans.lib.util.ruleengine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;

@AllArgsConstructor
public class LegacyFilesXmlRule implements BagValidatorRule {
    public enum Requirement {
        ROOT, FILE_ELEMENTS, FILEPATH_ATTRIBUTES, FORMAT, METADATA_NAMESPACES, ACCESS_RIGHTS
    }

    private static final Set<String> accessRights = Set.of("ANONYMOUS", "RESTRICTED_REQUEST", "NONE");
    private final XmlReader xmlReader;
    private final Requirement requirement;

    @Override
    public RuleResult validate(Path path) throws Exception {
        var root = xmlReader.readXmlFile(path.resolve("metadata/files.xml")).getDocumentElement();
        var files = xmlReader.xpathToStream(root, "*").toList();
        var errors = new ArrayList<String>();

        switch (requirement) {
            case ROOT -> {
                if (!"files".equals(root.getLocalName())
                    || root.getNamespaceURI() != null && !XmlReader.NAMESPACE_FILES_XML.equals(root.getNamespaceURI())) {
                    errors.add("files.xml: document element must be files, unqualified or in the DANS files namespace");
                }
            }
            case FILE_ELEMENTS -> {
                if (files.stream().anyMatch(n -> !"file".equals(n.getLocalName()) || !Objects.equals(root.getNamespaceURI(), n.getNamespaceURI()))) {
                    errors.add("files.xml: files must contain only file elements");
                }
            }
            case FILEPATH_ATTRIBUTES -> {
                if (files.stream().anyMatch(n -> !((Element) n).hasAttribute("filepath") || ((Element) n).getAttribute("filepath").isBlank())) {
                    errors.add("files.xml: every file must have a nonempty filepath attribute");
                }
            }
            case FORMAT -> {
                for (var file : files) {
                    if (xmlReader.xpathToStream(file, "dcterms:format").noneMatch(n -> !n.getTextContent().isBlank())) {
                        errors.add("files.xml: every file must have a dcterms:format containing its media type");
                    }
                }
            }
            case METADATA_NAMESPACES -> {
                // The schema checks its own extensions when the document uses the files namespace.
                if (!XmlReader.NAMESPACE_FILES_XML.equals(root.getNamespaceURI())) {
                    for (var file : files) {
                        if (xmlReader.xpathToStream(file, "*").anyMatch(n -> !isAllowedMetadata(n))) {
                            errors.add("files.xml: file metadata must use DC, DCTERMS, or permitted files-schema extensions");
                        }
                    }
                }
            }
            case ACCESS_RIGHTS -> {
                for (var file : files) {
                    var deprecated = xmlReader.xpathToStream(file, "dcterms:accessRights").toList();
                    var accessible = xmlReader.xpathToStream(file, "files:accessibleToRights").toList();
                    var visible = xmlReader.xpathToStream(file, "files:visibleToRights").toList();
                    if (deprecated.size() > 1 || accessible.size() > 1 || visible.size() > 1
                        || !deprecated.isEmpty() && (!accessible.isEmpty() || !visible.isEmpty())) {
                        errors.add("files.xml: use either one accessRights or accessibleToRights and/or visibleToRights");
                    }
                    if (xmlReader.xpathToStream(file, "dcterms:accessRights | files:accessibleToRights | files:visibleToRights")
                        .anyMatch(n -> !accessRights.contains(n.getTextContent().trim()))) {
                        errors.add("files.xml: access rights must be ANONYMOUS, RESTRICTED_REQUEST, or NONE");
                    }
                }
            }
        }
        return errors.isEmpty() ? RuleResult.ok() : RuleResult.error(errors);
    }

    private boolean isAllowedMetadata(Node node) {
        return XmlReader.NAMESPACE_DC.equals(node.getNamespaceURI()) || XmlReader.NAMESPACE_DCTERMS.equals(node.getNamespaceURI())
            || XmlReader.NAMESPACE_FILES_XML.equals(node.getNamespaceURI())
            && Set.of("accessibleToRights", "visibleToRights").contains(node.getLocalName());
    }
}
