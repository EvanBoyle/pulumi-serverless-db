import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { CallbackFunction } from "@pulumi/aws/lambda";
import { EventRuleEvent } from "@pulumi/aws/cloudwatch";
import { getS3Location } from "../../utils";
import { createPartitionDDLStatement } from "./partitionHelper";

export class HourlyPartitionRegistrar extends pulumi.ComponentResource {

    constructor(name: string, args: PartitionRegistrarArgs, opts?: pulumi.ComponentResourceOptions) {
        super("serverless:partitionregistrar", name, opts);
        const { dataWarehouseBucket, athenaResultsBucket, scheduleExpression, table, partitionKey } = args;
        const location = getS3Location(dataWarehouseBucket, table);

        const options  = { parent: this }; 

        const resultsBucket = athenaResultsBucket.arn.apply(a => `s3://${a.split(":::")[1]}`);

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
        }, options);

        const partitionGenLambdaAccess = new aws.iam.RolePolicyAttachment("partition-lambda-access", {
            role: partitionRole,
            policyArn: aws.iam.ManagedPolicies.AWSLambdaFullAccess
        }, options);

        const partitionGenAthenaAccess = new aws.iam.RolePolicyAttachment("partition-athena-access", {
            role: partitionRole,
            policyArn: aws.iam.ManagedPolicies.AmazonAthenaFullAccess
        }, options);

        const schedule = scheduleExpression ? scheduleExpression : `rate(1 hour)`;

        const cron = new aws.cloudwatch.EventRule("hourly-cron", {
            scheduleExpression: schedule
        }, options);

        cron.onEvent(`${table}-partitionregistrar`, new CallbackFunction('partition-callback', {
            role: partitionRole,
            callback: (event: EventRuleEvent) => {
                // create an athena client here, write the 
                const athena = require("athena-client");
                const clientConfig = {
                    bucketUri: resultsBucket.get()
                };
                const awsConfig = {
                    region: args.region
                };

                const client = athena.createClient(clientConfig, awsConfig);

                const query = createPartitionDDLStatement(args.database.name.get(), location.get(), partitionKey, event.time);

                client.execute(query, (err: Error) => {
                    if (err) {
                        throw err;
                    }
                })
            }
        }), options);
    }
}

export interface PartitionRegistrarArgs {
    table: string;
    partitionKey: string;
    dataWarehouseBucket: aws.s3.Bucket;
    athenaResultsBucket: aws.s3.Bucket;
    database: aws.glue.CatalogDatabase;
    region: string;
    scheduleExpression?: string; // TODO: we should remove this. It's useful in active development, but users would probably never bother. 
}