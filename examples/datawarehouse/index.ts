import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { EventRuleEvent } from "@pulumi/aws/cloudwatch";
import { CallbackFunction } from "@pulumi/aws/lambda";
import { ServerlessDataWarehouse, StreamingInputTableArgs } from "../../lib/datawarehouse";

// app specific config
const config = new pulumi.Config();
const awsConfig = new pulumi.Config("aws")
const region = awsConfig.require("region");
const stage = config.require("stage");
const shards: { [key: string]: number } = config.requireObject("shards");
const isDev = config.get("dev");
const cronUnit = isDev ? "minute" : "hour";
const scheduleExpression  = `rate(1 ${cronUnit})`;

// dw w/ streaming input table
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
    inputStreamShardCount: shards[stage],
    region,
    scheduleExpression
}

const dataWarehouse = new ServerlessDataWarehouse("analytics_dw")
    .withStreamingInputTable(logsTableName, logsTableArgs);

const logsInputStream = dataWarehouse.getInputStream(logsTableName);
export const streamName = logsInputStream.name;

// generate test events
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
