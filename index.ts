import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import { EventRuleEvent } from "@pulumi/aws/cloudwatch";
import * as moment from "moment-timezone";
import { CallbackFunction } from "@pulumi/aws/lambda";
import { createPartitionDDLStatement } from "./athena/partitionHelper";

const config = new pulumi.Config();
const awsConfig = new pulumi.Config("aws")
const region = awsConfig.require("region");
const stage = config.require("stage");
const shards: { [key: string]: number } = config.requireObject("shards");

const dataWarehouseBucket = new aws.s3.Bucket("serverless-db-bucket");
const athenaResultsBucket = new aws.s3.Bucket("athena-results");

const db = new aws.glue.CatalogDatabase("severless-db", {
    name: "serverlessdb"
});

const location = dataWarehouseBucket.arn.apply(a => `s3://${a.split(":::")[1]}`);

const table = new aws.glue.CatalogTable("logs", {
    name: "logs",
    databaseName: db.name,
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
        columns: [
            {
                name: "id",
                type: "string"
            },
            {
                name: "session_id",
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

export const streamName = kinesis.name;
const resultsBucket = athenaResultsBucket.arn.apply( a => `s3://${a.split(":::")[1]}`);

let lambdaAssumeRolePolicy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Principal": {
                "Service": "lambda.amazonaws.com",
            },
            "Effect": "Allow",
            "Sid": "",
        },
    ],
};

const partitionRole = new aws.iam.Role("partitionLambdaRole", {
    assumeRolePolicy: JSON.stringify(lambdaAssumeRolePolicy)
});

const partitionGenLambdaAccess = new aws.iam.RolePolicyAttachment("partition-lambda-access", {
    role: partitionRole,
    policyArn: aws.iam.ManagedPolicies.AWSLambdaFullAccess
});

const partitionGenAthenaAccess = new aws.iam.RolePolicyAttachment("partition-athena-access", {
    role: partitionRole,
    policyArn: aws.iam.ManagedPolicies.AmazonAthenaFullAccess
});

const isDev = config.get("dev");
const cronUnit = isDev ? "minute" : "hour";
const scheduleExpression  = `rate(1 ${cronUnit})`;

const cron = new aws.cloudwatch.EventRule("hourly-cron", {
    scheduleExpression
});

cron.onEvent("partition-registrar", new CallbackFunction('partition-callback', {
    role: partitionRole,
    callback: (event: EventRuleEvent) => {
        // create an athena client here, write the 
        const athena = require("athena-client");
        const clientConfig = {
            bucketUri: resultsBucket.get()
        };
        const awsConfig = {
            region: region
        };
    
        const client = athena.createClient(clientConfig, awsConfig);
    
        const query = createPartitionDDLStatement(db.name.get(), location.get(), event.time);

        client.execute(query, (err: Error) => {
            if (err) {
                throw err;
            }
        })
    }
}));

const eventGenRole = new aws.iam.Role("eventGenLambdaRole", {
    assumeRolePolicy: JSON.stringify(lambdaAssumeRolePolicy),
});

const eventGenLambdaAccess = new aws.iam.RolePolicyAttachment("event-gen-lambda-access", {
    role: eventGenRole,
    policyArn: aws.iam.ManagedPolicies.AWSLambdaFullAccess,
});

const eventGenKinesisAccess = new aws.iam.RolePolicyAttachment("event-gen-kinesis-access", {
    role: eventGenRole,
    policyArn: aws.iam.ManagedPolicies.AmazonKinesisFullAccess,
});


const eventCron = new aws.cloudwatch.EventRule("event-gen-cron", {
    scheduleExpression: "rate(1 minute)",
});

eventCron.onEvent("event-generator", new CallbackFunction('event-gen-callback', {
    role: eventGenRole,
    callback: (event: EventRuleEvent) => {
        const AWS = require("aws-sdk");
        const uuid = require("uuid/v4")
        const kinesis = new AWS.Kinesis();
        const records: any = [];

        const sessionId = uuid();
        const eventId = uuid();
        const record = {
            Data: JSON.stringify({
                id: eventId,
                session_id: sessionId,
                message: "this is a message",
                event_type: "impression",
            }),
            PartitionKey: sessionId
        };
        records.push(record);

        kinesis.putRecords({
            Records: records,
            StreamName: streamName.get()
        }, (err: any) => {
            if (err) {
                console.error(err)
            }
        });
    }

}));
