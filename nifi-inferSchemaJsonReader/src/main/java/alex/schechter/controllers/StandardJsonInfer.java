/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package alex.schechter.controllers;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.kitesdk.data.spi.JsonUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tags({ "alex", "json", "infer", "schema"})
@CapabilityDescription(" ControllerService for inferring json schema.")
public class StandardJsonInfer extends AbstractControllerService implements RecordReaderFactory {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<>(super.getSupportedPropertyDescriptors());
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        in.mark(0);
        String content = IOUtils.toString(in, StandardCharsets.UTF_8);
        final RecordSchema schema1 = customGetSchema(content, logger);
        in.reset();


        return new JsonTreeRowRecordReader(in, logger, schema1, "", "", "");
    }

    public RecordSchema customGetSchema(final String content, final ComponentLog logger) throws IOException {
        String schema = JsonUtil.inferSchema(JsonUtil.parse(content), "myschema").toString();
        JsonObject obj = new JsonParser().parse(schema).getAsJsonObject();
        if (obj.get("type").getAsString().equals("array")) {
            obj = obj.getAsJsonObject("items");
        }
        String updatedSting = obj.toString();

        final Schema avroSchema = new Schema.Parser().parse(updatedSting);
        return AvroTypeUtil.createSchema(avroSchema);
    }
}

