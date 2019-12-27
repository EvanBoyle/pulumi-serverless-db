import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import { EventRuleEvent } from "@pulumi/aws/cloudwatch";
import * as moment from "moment-timezone";
import { CallbackFunction } from "@pulumi/aws/lambda";
import { createPartitionDDLStatement } from "../../lib/athena/partitionHelper";
import { getS3Location } from "../../utils";
import { ServerlessDataWarehouse, StreamingInputTableArgs } from "../../lib/datawarehouse";


// app specific config
const config = new pulumi.Config();
const awsConfig = new pulumi.Config("aws")
const region = awsConfig.require("region");
const stage = config.require("stage");
const shards: { [key: string]: number } = config.requireObject("shards");

// create the table, s3 bucket, etc
const columns = [
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
];

const logsTableName = "logs";
const logsTableArgs: StreamingInputTableArgs = {
    columns,
    inputStreamShardCount: shards[stage]
}

const dataWarehouse = new ServerlessDataWarehouse("analytics_dw")
    .withStreamingInputTable(logsTableName, logsTableArgs);

const { dataWarehouseBucket, queryResultsBucket, database } = dataWarehouse;
const logsInputStream = dataWarehouse.getInputStream(logsTableName);

const location = getS3Location(dataWarehouseBucket);

// register partitions
export const streamName = logsInputStream.name;
const resultsBucket = queryResultsBucket.arn.apply( a => `s3://${a.split(":::")[1]}`);

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
    
        const query = createPartitionDDLStatement(database.name.get(), location.get(), event.time);

        client.execute(query, (err: Error) => {
            if (err) {
                throw err;
            }
        })
    }
}));

// generate test events
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
