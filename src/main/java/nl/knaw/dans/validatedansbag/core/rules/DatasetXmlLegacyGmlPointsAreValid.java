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

import nl.knaw.dans.lib.util.ruleengine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;
import org.w3c.dom.Node;

import java.nio.file.Path;
import java.util.Arrays;

public class DatasetXmlLegacyGmlPointsAreValid extends DatasetXmlGmlPointsHaveAtLeastTwoValues {
    private final XmlReader xmlReader;

    public DatasetXmlLegacyGmlPointsAreValid(XmlReader xmlReader) {
        super(xmlReader);
        this.xmlReader = xmlReader;
    }

    @Override
    public RuleResult validate(Path path) throws Exception {
        var result = super.validate(path);
        if (result.getStatus() != RuleResult.Status.SUCCESS) {
            return result;
        }
        var document = xmlReader.readXmlFile(path.resolve("metadata/dataset.xml"));
        var points = xmlReader.xpathToStream(document, "//gml:Point/gml:pos | //gml:lowerCorner | //gml:upperCorner").toList();
        for (var point : points) {
            var values = Arrays.stream(point.getTextContent().trim().split("\\s+")).mapToDouble(Double::parseDouble).toArray();
            if (Arrays.stream(values).anyMatch(v -> !Double.isFinite(v))) {
                return RuleResult.error("dataset.xml: point coordinates must be finite numbers");
            }
            var srsName = getSrsName(point);
            if (("http://www.opengis.net/def/crs/EPSG/0/28992".equals(srsName) || "urn:ogc:def:crs:EPSG::28992".equals(srsName))
                && !(values[0] >= -7000 && values[0] <= 300000 && values[1] >= 289000 && values[1] <= 629000)) {
                return RuleResult.error("dataset.xml: point is outside RD bounds: " + point.getTextContent());
            }
        }
        return result;
    }

    private String getSrsName(Node node) {
        for (var current = node; current != null; current = current.getParentNode()) {
            var attributes = current.getAttributes();
            if (attributes != null && attributes.getNamedItem("srsName") != null) {
                return attributes.getNamedItem("srsName").getTextContent();
            }
        }
        return null;
    }
}
