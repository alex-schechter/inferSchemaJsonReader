# inferSchemaJsonReader
This repo is a custom nifi ControllerService to better infer the schema of the records.

The only change from the regular inferSchema strategy is this one is using the avro library to infer the schema.

To build:
1. mvn clean package -DskipTests=true
2. cp nifi-inferSchemaJsonReader-nar/target/nifi-inferSchemaJsonReader-nar-1.0.nar <nifi /lib path>
3. cp nifi-inferSchemaJsonReader-api-nar/target/nifi-inferSchemaJsonReader-api-nar-1.0.nar <nifi /lib path>
4. restart nifi

