import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as gcp from "@pulumi/gcp"
import * as awsx from "@pulumi/awsx";
import { EventRuleEvent } from "@pulumi/aws/cloudwatch";

const config = new pulumi.Config();
const awsConfig = new pulumi.Config("aws")
const region = awsConfig.require("region");
const stage = config.require("stage");
const shards: { [key: string]: number } = config.requireObject("shards");

const dataWarehouseBucket = new aws.s3.Bucket("serverless-db-bucket");
const athenaResultsBucket = new aws.s3.Bucket("athena-results");

const adb = new gcp.sql.DatabaseInstance('app-db', {
    databaseVersion: 'POSTGRES_11',
    region: 'us-central1',
    settings: {
      tier: 'db-f1-micro',
      diskType: 'PD_HDD',
    },
  });

const db = new aws.glue.CatalogDatabase("severless-db", {
    name: "serverlessdb"
});

const location = dataWarehouseBucket.arn.apply( a => `s3://${a.split(":::")[1]}`);

const table = new aws.glue.CatalogTable("logs", {
    name: "logs",
    databaseName: db.name,
    tableType: "EXTERNAL_TABLE",
    storageDescriptor: {
        location,
        inputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        outputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serDeInfo: {
            parameters: {"serialization.format": "1"},
            name: "ParquetHiveSerDe",
            serializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        },
        columns: [
            {
                name: "id",
                type: "string"
            },
            {
                name: "message",
                type: "string"
            },
            {
                name: "event_type",
                type: "string"
            }
        ]
    },
    partitionKeys: [
        {
            name: "inserted_at",
            type: "string"
        }
    ]
});

const kinesis = new aws.kinesis.Stream("incoming-events", {
    shardCount: shards[stage],
});

let assumeRolePolicy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Principal": {
                "Service": "firehose.amazonaws.com",
            },
            "Effect": "Allow",
            "Sid": "",
        },
    ],
};

const role = new aws.iam.Role("firehoseRose", {
    assumeRolePolicy: JSON.stringify(assumeRolePolicy),
});

let kinesisAccess = new aws.iam.RolePolicyAttachment("kinesis-access", {
    role,
    policyArn: aws.iam.ManagedPolicies.AmazonKinesisFullAccess,
});

let s3Access = new aws.iam.RolePolicyAttachment("s3-access", {
    role,
    policyArn: aws.iam.ManagedPolicies.AmazonS3FullAccess,
});

const gluePolicy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
            ],
            "Resource": "*"   
        }
    ]
};

let glueAccess = new aws.iam.RolePolicy("glue-policy", { role: role, policy: JSON.stringify(gluePolicy) });

let logGroup = new aws.cloudwatch.LogGroup("/aws/firehose/parquet-stream", {
    retentionInDays: 7,
});

let logStream = new aws.cloudwatch.LogStream("serverless-db-s3-delivery", {
    logGroupName: logGroup.name
});

const parquetDeliveryStream = new aws.kinesis.FirehoseDeliveryStream("parquet-delivery-stream", {
    kinesisSourceConfiguration: {
        kinesisStreamArn: kinesis.arn,
        roleArn: role.arn
    },
    destination: "extended_s3", // wish there was intellisense on these values. 
    extendedS3Configuration: {
        cloudwatchLoggingOptions: {
            logGroupName: logGroup.name,
            enabled: true,
            logStreamName: logStream.name,
        },
        bucketArn: dataWarehouseBucket.arn,
        bufferInterval: 60,
        bufferSize: 64,
        roleArn: role.arn, 
        dataFormatConversionConfiguration: {
            inputFormatConfiguration: {
                deserializer: {
                    openXJsonSerDe: {}
                }
            },
            outputFormatConfiguration: {
                serializer: {
                    parquetSerDe: {}
                }
            },
            schemaConfiguration: {
                databaseName: db.name,
                tableName: table.name,
                roleArn: role.arn
            }
        }
    }
});

//export const deliveryStreamArn = parquetDeliveryStream.arn;
export const streamName = kinesis.name;


// TODO - register the partitions after we can reliabily write events into the kinesis stream
// and use athena from the consolee

// const cron = new aws.cloudwatch.EventRule("hourly-cron", {
//     scheduleExpression: "rate(1 hour)"
// });

// cron.onEvent("partition-registrar", (event: EventRuleEvent) => {
//     // create an athena client here, write the 
//     const athena = require("athena-client");
//     const clientConfig = {
//         bucketUri: `s3://${athenaResultsBucket.arn.apply((arn => arn.split(':::')[1]))}`
//     };
//     const awsConfig = {
//         region: region
//     };

//     const client = athena.createClient(clientConfig, awsConfig);

//     client.execute('TODO QUERY', (err: Error) => {
//         if (err) {
//             throw err;
//         }
//     })
// });
