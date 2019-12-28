import * as aws from "@pulumi/aws";
import { EventRuleEvent } from "@pulumi/aws/cloudwatch";
import { CallbackFunction } from "@pulumi/aws/lambda";

// TODO create component resource for this
export const createEventGenerator = (eventType: string, inputStreamName: string) => {
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
                    event_type: eventType,
                }),
                PartitionKey: sessionId
            };
            records.push(record);

            kinesis.putRecords({
                Records: records,
                StreamName: inputStreamName
            }, (err: any) => {
                if (err) {
                    console.error(err)
                }
            });
        }

    }));
};