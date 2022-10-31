/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kipshi.alluxio;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.kipshi.alluxio.iceberg.IcebergClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.UUID;

public class HiveIcebergConnect {

    public static void main(String[] args) throws Exception {
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        HiveCatalog catalog = IcebergClient.create();

        TableIdentifier identifier = TableIdentifier.of("default", "logs1");

        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .identity("level")
                .build();

        HiveTableOperations ops = (HiveTableOperations) catalog.newTableOps(identifier);
        if (ops.current() != null) {
            throw new AlreadyExistsException("Table already exists: %s", identifier);
        }
        Field metaClientField = HiveTableOperations.class.getDeclaredField("metaClients");
        metaClientField.setAccessible(true);
        ClientPool<IMetaStoreClient, TException> metaClients = (ClientPool) metaClientField.get(ops);

        Database databaseData = metaClients.run(client -> client.getDatabase(identifier.namespace().levels()[0]));
        System.out.println("locationUrl:" + databaseData.getLocationUri());

        Map<String, String> tableProperties = Maps.newHashMap();
        //get baseLocation
        Method method = catalog.getClass().getDeclaredMethod("defaultWarehouseLocation", TableIdentifier.class);
        method.setAccessible(true);
        String baseLocation = (String) method.invoke(catalog, identifier);
        System.out.println("baseLocation:" + baseLocation);
        tableProperties.putAll(tableOverrideProperties());
        SortOrder sortOrder = SortOrder.unsorted();
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
        //parse filesystem path
        String codecName = metadata.property(
                TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
        System.out.println("codecName:" + codecName);
        String fileExtension = TableMetadataParser.getFileExtension(codecName);
        System.out.println("fileExtension:" + fileExtension);
        String metadataLocation = metadata.properties()
                .get(TableProperties.WRITE_METADATA_LOCATION);
        System.out.println("metadataLocation:" + metadataLocation);
        System.out.println("metadata.location:" + metadata.location());
        //reset location
        String location = metadata.location();
        location = location.replace("hdfs://hdfsCluster", "alluxio://172.17.0.66:19998,172.17.0.171:19998/iceberg");
        Field locationField = metadata.getClass().getDeclaredField("location");
        locationField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(locationField, locationField.getModifiers() & ~Modifier.FINAL);
        locationField.set(metadata, location);
        System.out.println("metadata.location after set:" + metadata.location());

        String filename = String.format("%05d-%s%s", 0, UUID.randomUUID(), fileExtension);
        System.out.println("fileName:" + filename);
        String tableMetaFilePath;

        if (metadataLocation != null) {
            tableMetaFilePath = String.format("%s/%s", metadataLocation, filename);
        } else {
            tableMetaFilePath = String.format("%s/%s/%s", metadata.location(), "metadata", filename);
        }
        System.out.println("tableMetaFilePath:" + tableMetaFilePath);

        try {
            ops.commit(null, metadata);
        } catch (CommitFailedException ignored) {
            throw new AlreadyExistsException("Table was created concurrently: %s", identifier);
        }
        //get table name
        method = BaseMetastoreCatalog.class.getDeclaredMethod("fullTableName", String.class, TableIdentifier.class);
        method.setAccessible(true);
        String tableName = (String) method.invoke(catalog, catalog.name(), identifier);
        System.out.println("tableName:" + tableName);
        Table table = new BaseTable(ops, tableName);

//        Table table = catalog.createTable(identifier, schema, spec);
        System.out.println("table: " + table.name());
        TableScan scan = table.newScan();
        System.out.println("schema: " + scan.schema().toString());
        table.updateSchema()
                .addColumn("count", Types.LongType.get())
                .commit();
        scan = table.newScan();
        System.out.println("schema after update: " + scan.schema().toString());

    }

    private static Map<String, String> tableOverrideProperties() {
        Map<String, String> tableOverrideProperties =
                PropertyUtil.propertiesWithPrefix(ImmutableMap.of(), CatalogProperties.TABLE_OVERRIDE_PREFIX);
        System.out.println(
                "Table properties enforced at catalog level through catalog properties: " + tableOverrideProperties);
        return tableOverrideProperties;
    }


}
