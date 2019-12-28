import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { input } from "@pulumi/aws/types";
import { DataPipeline } from "aws-sdk";

export class InputStream extends pulumi.ComponentResource {

    public inputStream: aws.kinesis.Stream;
    
    constructor(name: string, args: InputStreamArgs, opts?: pulumi.ComponentResourceOptions) {
        super("serverless:inputstream", name, opts);
        
        const kinesis = new aws.kinesis.Stream(`${name}-input-stream`, {
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
        
        const role = new aws.iam.Role(`${name}-firehoseRose`, {
            assumeRolePolicy: JSON.stringify(assumeRolePolicy),
        });
        
        let kinesisAccess = new aws.iam.RolePolicyAttachment(`${name}-kinesis-access`, {
            role,
            policyArn: aws.iam.ManagedPolicies.AmazonKinesisFullAccess,
        });
        
        let s3Access = new aws.iam.RolePolicyAttachment(`${name}-s3-access`, {
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
        
        let glueAccess = new aws.iam.RolePolicy(`${name}-glue-policy`, { role: role, policy: JSON.stringify(gluePolicy) });
        
        let logGroup = new aws.cloudwatch.LogGroup(`/aws/firehose/${name}/parquet-stream`, {
            retentionInDays: 7,
        });
        
        let logStream = new aws.cloudwatch.LogStream(`${name}-serverless-db-s3-delivery`, {
            logGroupName: logGroup.name
        });
        
        const parquetDeliveryStream = new aws.kinesis.FirehoseDeliveryStream(`${name}-parquet-delivery-stream`, {
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
                prefix: args.tableName + '/',
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
}

export interface InputStreamArgs {
    databaseName: pulumi.Input<string>;
    tableName: pulumi.Input<string>;
    destinationBucket: aws.s3.Bucket;
    shardCount: number;
    // buffering hints
    // logging config
}