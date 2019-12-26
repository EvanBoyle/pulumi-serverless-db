import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { input } from "@pulumi/aws/types";
import { DataPipeline } from "aws-sdk";

export class AwsServerlessDataPipeline extends pulumi.ComponentResource {

    public inputStream: aws.kinesis.Stream;
    
    constructor(name: string, args: DataPipelineArgs) {
        super("serverless:dataPipeline", name, {});

        this.validateArgs(args);
        
        const kinesis = new aws.kinesis.Stream("incoming-events", {
            shardCount: args.shardCount,
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
            destination: "extended_s3",
            extendedS3Configuration: {
                cloudwatchLoggingOptions: {
                    logGroupName: logGroup.name,
                    enabled: true,
                    logStreamName: logStream.name,
                },
                bucketArn: args.destinationBucket.arn,
                bufferInterval: 60,// todo make configurable 
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
                        databaseName: args.databaseName,
                        tableName: args.tableName,
                        roleArn: role.arn
                    }
                }
            }
        });
        this.inputStream = kinesis;
        this.registerOutputs({
            inputStream: kinesis
        })
    }

    private validateArgs(args: DataPipelineArgs) {
        // TODO implement
        const valid = true; 
        if(!valid) {
            throw new Error("Failed to validate 'DataWarehouseArgs.");
        }
    }
}

export interface DataPipelineArgs {
    databaseName: pulumi.Input<string>;
    tableName: pulumi.Input<string>;
    destinationBucket: aws.s3.Bucket;
    shardCount: number;
}