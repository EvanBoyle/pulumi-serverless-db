import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { input } from "@pulumi/aws/types";
import { getS3Location } from "../utils";
import { InputStream, InputStreamArgs } from "./inputStream";
import { HourlyPartitionRegistrar, PartitionRegistrarArgs } from "./partitionRegistrar";

export class ServerlessDataWarehouse extends pulumi.ComponentResource {

    public dataWarehouseBucket: aws.s3.Bucket;
    public queryResultsBucket: aws.s3.Bucket;
    public database: aws.glue.CatalogDatabase;
    private tables: { [key: string]: aws.glue.CatalogTable } = {};
    private inputStreams: { [key: string]: aws.kinesis.Stream } = {};

    /**
     * TODO: 
     * let's expose some helpful utilities here:
     * GetAthenaQueryIAMPolicy: return the JSON required to query against this thing.
     * * - support multiple tables - use convention to make this happen. 
     * Add option for encryption
     * 
     */
    constructor(name: string, args?: DataWarehouseArgs) {
        super("serverless:datawarehouse", name, {});

        const dataWarehouseBucket = new aws.s3.Bucket("datawarehouse-bucket", undefined, { parent: this });
        const queryResultsBucket = new aws.s3.Bucket("query-results-bucket", undefined, { parent: this });

        const dwArgs = args ? args : {};


        const database = dwArgs.database ? dwArgs.database : new aws.glue.CatalogDatabase(name, {
            name
        }, { parent: this });


        this.dataWarehouseBucket = dataWarehouseBucket;
        this.queryResultsBucket = queryResultsBucket;
        this.database = database;
    }

    public withTable(name: string, args: TableArgs): ServerlessDataWarehouse {
        if (this.tables[name]) {
            throw new Error(`Duplicate table! Name: ${name}`);
        }
        const table = this.createTable(name, args.columns, args.partitionKeys);
        this.tables[name] = table;

        return this;
    }

    public withStreamingInputTable(name: string, args: StreamingInputTableArgs): ServerlessDataWarehouse {
        const { partitionKeyName, columns, inputStreamShardCount } = args;
        const partitionKey = partitionKeyName ? partitionKeyName : "inserted_at";

        const partitionKeys: input.glue.CatalogTablePartitionKey[] = [{
            name: partitionKey,
            type: "string"
        }];
        
        const tableArgs: TableArgs = {
            columns,
            partitionKeys,
        };

        this.withTable(name, tableArgs);

        const streamArgs: InputStreamArgs = {
            destinationBucket: this.dataWarehouseBucket,
            shardCount: inputStreamShardCount,
            databaseName: this.database.name,
            tableName: name
        };
        
        const { inputStream } = new InputStream(`inputstream-${name}`, streamArgs, { parent: this});
        this.inputStreams[name] = inputStream;

        const registrarArgs: PartitionRegistrarArgs = {
            database: this.database,
            partitionKey,
            region: args.region,
            dataWarehouseBucket: this.dataWarehouseBucket,
            athenaResultsBucket: this.queryResultsBucket,
            table: name,
            scheduleExpression: args.scheduleExpression,
        };
        const partitionRegistrar = new HourlyPartitionRegistrar(`${name}-partitionregistrar`, registrarArgs, { parent: this });

        return this;
    }

    public withBatchInputTable() { /* TODO */ }

    public getTable(name: string): aws.glue.CatalogTable {
        const table = this.tables[name]; 
        if(!table) {
            throw new Error(`Table '${name}' does not exist.`);
        }

        return table;
    }
    
    public listTables(): string[] {
        return Object.keys(this.tables);
    }

    public getInputStream(tableName: string): aws.kinesis.Stream {
        const stream = this.inputStreams[tableName]; 
        if(!stream) {
            throw new Error(`Input stream for table '${tableName}' does not exist.`);
        }

        return stream;
    }

    private createTable(name: string, columns: input.glue.CatalogTableStorageDescriptorColumn[], partitionKeys?: input.glue.CatalogTablePartitionKey[]): aws.glue.CatalogTable {
        const location = getS3Location(this.dataWarehouseBucket, name);
        return new aws.glue.CatalogTable(name, {
            name: name,
            databaseName: this.database.name,
            tableType: "EXTERNAL_TABLE",
            storageDescriptor: {
                location,
                inputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                outputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                serDeInfo: {
                    parameters: { "serialization.format": "1" },
                    name: "ParquetHiveSerDe",
                    serializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
                columns
            },
            partitionKeys,
        }, { parent: this });
    }

}

export interface DataWarehouseArgs {
    database?: aws.glue.CatalogDatabase;
}

export interface TableArgs {
    columns: input.glue.CatalogTableStorageDescriptorColumn[];
    partitionKeys?: input.glue.CatalogTablePartitionKey[];
}

export interface StreamingInputTableArgs {
    columns: input.glue.CatalogTableStorageDescriptorColumn[]
    inputStreamShardCount: number;
    region: string;
    partitionKeyName?: string;
    scheduleExpression?: string; // TODO: we should remove this. It's useful in active development, but users would probably never bother. 
}