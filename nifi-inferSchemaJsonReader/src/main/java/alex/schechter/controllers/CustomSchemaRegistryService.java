package alex.schechter.controllers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;

public abstract class CustomSchemaRegistryService extends AbstractControllerService {
    private volatile ConfigurationContext configurationContext;
    protected volatile SchemaAccessStrategy schemaAccessStrategy;
    private static InputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);
    private final List<AllowableValue> strategyList;

    public CustomSchemaRegistryService() {
        this.strategyList = Collections.unmodifiableList(Arrays.asList(SchemaAccessUtils.SCHEMA_NAME_PROPERTY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY, SchemaAccessUtils.HWX_SCHEMA_REF_ATTRIBUTES, SchemaAccessUtils.HWX_CONTENT_ENCODED_SCHEMA, SchemaAccessUtils.CONFLUENT_ENCODED_SCHEMA));
    }

    protected PropertyDescriptor getSchemaAcessStrategyDescriptor() {
        return this.getPropertyDescriptor(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY.getName());
    }

    protected PropertyDescriptor buildStrategyProperty(AllowableValue[] values) {
        return (new Builder()).fromPropertyDescriptor(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY).allowableValues(values).defaultValue(this.getDefaultSchemaAccessStrategy().getValue()).build();
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList(2);
        AllowableValue[] strategies = (AllowableValue[])this.getSchemaAccessStrategyValues().toArray(new AllowableValue[0]);
        properties.add(this.buildStrategyProperty(strategies));
        properties.add(SchemaAccessUtils.SCHEMA_REGISTRY);
        properties.add(SchemaAccessUtils.SCHEMA_NAME);
        properties.add(SchemaAccessUtils.SCHEMA_VERSION);
        properties.add(SchemaAccessUtils.SCHEMA_BRANCH_NAME);
        properties.add(SchemaAccessUtils.SCHEMA_TEXT);
        return properties;
    }

    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
    }

    @OnEnabled
    public void storeSchemaAccessStrategy(ConfigurationContext context) {
        this.configurationContext = context;
        SchemaRegistry schemaRegistry = (SchemaRegistry)context.getProperty(SchemaAccessUtils.SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
        PropertyDescriptor descriptor = this.getSchemaAcessStrategyDescriptor();
        String schemaAccess = context.getProperty(descriptor).getValue();
        this.schemaAccessStrategy = this.getSchemaAccessStrategy(schemaAccess, schemaRegistry, context);
    }

    protected ConfigurationContext getConfigurationContext() {
        return this.configurationContext;
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy() {
        return this.schemaAccessStrategy;
    }

    public final RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        SchemaAccessStrategy accessStrategy = this.getSchemaAccessStrategy();
        if (accessStrategy == null) {
            throw new SchemaNotFoundException("Could not determine the Schema Access Strategy for this service");
        } else {
            return this.getSchemaAccessStrategy().getSchema(variables, contentStream, readSchema);
        }
    }

    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return this.getSchema(variables, EMPTY_INPUT_STREAM, readSchema);
    }

    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        String schemaAccessStrategy = validationContext.getProperty(this.getSchemaAcessStrategyDescriptor()).getValue();
        return SchemaAccessUtils.validateSchemaAccessStrategy(validationContext, schemaAccessStrategy, this.getSchemaAccessStrategyValues());
    }

    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        return this.strategyList;
    }

    protected Set<SchemaField> getSuppliedSchemaFields(ValidationContext validationContext) {
        String accessStrategyValue = validationContext.getProperty(this.getSchemaAcessStrategyDescriptor()).getValue();
        SchemaRegistry schemaRegistry = (SchemaRegistry)validationContext.getProperty(SchemaAccessUtils.SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
        SchemaAccessStrategy accessStrategy = this.getSchemaAccessStrategy(accessStrategyValue, schemaRegistry, validationContext);
        if (accessStrategy == null) {
            return EnumSet.noneOf(SchemaField.class);
        } else {
            Set<SchemaField> suppliedFields = accessStrategy.getSuppliedSchemaFields();
            return suppliedFields;
        }
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy(String allowableValue, SchemaRegistry schemaRegistry, PropertyContext context) {
        return allowableValue == null ? null : SchemaAccessUtils.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }
}