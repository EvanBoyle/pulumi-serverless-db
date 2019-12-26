import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { input } from "@pulumi/aws/types";
import { getS3Location } from "../../../utils";

export class AwsServerlessDataWarehouse extends pulumi.ComponentResource {

    public dataWarehouseBucket: aws.s3.Bucket;
    public queryResultsBucket: aws.s3.Bucket;
    public database: aws.glue.CatalogDatabase;
    public table: aws.glue.CatalogTable
    /**
     * TODO: 
     * let's expose some helpful utilities here:
     * GetAthenaQueryIAMPolicy: return the JSON required to query against this thing.
     * * - support multiple tables - use convention to make this happen. 
     * Add option for encryption
     * 
     */
    constructor(name: string, args: DataWarehouseArgs) {
        super("serverless:datawarehouse", name, {});

        this.validateArgs(args);
        const dataWarehouseBucket = new aws.s3.Bucket("datawarehouse-bucket");
        const queryResultsBucket = new aws.s3.Bucket("query-results-bucket");

        const location = getS3Location(dataWarehouseBucket);
        const database = new aws.glue.CatalogDatabase("severless-db", {
            name: "serverlessdb" // todo - add randomness here. 
        });

        const partitionKeyName = args.partitionKeyName ? args.partitionKeyName : "inserted_at";

        
        const table = new aws.glue.CatalogTable("logs", {
            name: "logs",
            databaseName: database.name,
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
                columns: args.columns
            },
            partitionKeys: [
                {
                    name: partitionKeyName,
                    type: "string"
                }
            ]
        });

        this.dataWarehouseBucket = dataWarehouseBucket;
        this.queryResultsBucket = queryResultsBucket;
        this.database = database;
        this.table = table;

        this.registerOutputs({
            dataWarehouseBucket,
            queryResultsBucket,
            database,
            table
        })
    }

    private validateArgs(args: DataWarehouseArgs) {
        // TODO implement
        const valid = true; 
        if(!valid) {
            throw new Error("Failed to validate 'DataWarehouseArgs.");
        }
    }
}

export interface DataWarehouseArgs {
    tableName: string;
    database?: aws.glue.CatalogDatabase;
    columns: input.glue.CatalogTableStorageDescriptorColumn[];
    partitionKeyName?: string;
}