import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { S3 } from "aws-sdk";

import { ARN } from "@pulumi/aws";
import { EventRuleEvent } from "@pulumi/aws/cloudwatch";
import * as moment from "moment-timezone";

import { ServerlessDataWarehouse, StreamingInputTableArgs, BatchInputTableArgs, TableArgs } from "../../lib/datawarehouse";
import { createEventGenerator } from "./eventGenerator";

// app specific config
const config = new pulumi.Config();
const awsConfig = new pulumi.Config("aws")
const region = awsConfig.require("region");
const stage = config.require("stage");
const shards: { [key: string]: number } = config.requireObject("shards");
const isDev = config.get("dev") === 'true';
const cronUnit = isDev ? "minute" : "hour";
const scheduleExpression = `rate(1 ${cronUnit})`;

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

const impressionsTableName = "impressions";
const clicksTableName = "clicks";

const genericTableArgs: StreamingInputTableArgs = {
    columns,
    inputStreamShardCount: shards[stage],
    region,
    scheduleExpression
}

// create two tables with kinesis input streams, writing data into hourly partitions in S3. 
const dataWarehouse = new ServerlessDataWarehouse("analytics_dw_integration", { isDev })
    .withStreamingInputTable(impressionsTableName, genericTableArgs)
    .withStreamingInputTable(clicksTableName, genericTableArgs);

const impressionsInputStream = dataWarehouse.getInputStream(impressionsTableName);
const clicksInputStream = dataWarehouse.getInputStream(clicksTableName);


// Export a batch of outputs from the first two tables.
export const impressionInputStream = impressionsInputStream.name;
export const clickInputStream = clicksInputStream.name;
export const databaseName = dataWarehouse.database.name;
export const impressionTableName = dataWarehouse.getTable(impressionsTableName).name;
export const clickTableName = dataWarehouse.getTable(clicksTableName).name;
export const athenaResultsBucket = dataWarehouse.queryResultsBucket.bucket;

const dwBucket = dataWarehouse.dataWarehouseBucket.bucket

// Configure batch input table 'aggregates'
const aggregateTableName = "aggregates";

const aggregateTableColumns = [
    {
        name: "event_type",
        type: "string"
    },
    {
        name: "count",
        type: "int"
    }
];

const aggregationFunction = async (event: EventRuleEvent) => {
    const athena = require("athena-client");
    const bucketUri = `s3://${athenaResultsBucket.get()}`;
    const clientConfig = {
        bucketUri
    };
    const awsConfig = {
        region
    };
    const athenaClient = athena.createClient(clientConfig, awsConfig);
    let date = moment(event.time);
    const partitionKey = date.utc().format("YYYY/MM/DD/HH");
    const getAggregateQuery = (table: string) => `select count(*) from ${databaseName.get()}.${table} where inserted_at='${partitionKey}'`;
    const clicksPromise = athenaClient.execute(getAggregateQuery(clicksTableName)).toPromise();
    const impressionsPromise = athenaClient.execute(getAggregateQuery(impressionsTableName)).toPromise();

    const clickRows = await clicksPromise;
    const impressionRows = await impressionsPromise;
    const clickCount = clickRows.records[0]['_col0'];
    const impressionsCount = impressionRows.records[0]['_col0'];
    const data = `{ "event_type": "${clicksTableName}", "count": ${clickCount} }\n{ "event_type": "${impressionsTableName}", "count": ${impressionsCount} }`;
    const s3Client = new S3();
    await s3Client.putObject({
        Bucket: dwBucket.get(),
        Key: `${aggregateTableName}/${partitionKey}/results.json`,
        Body: data
    }).promise();
};

const policyARNsToAttach: pulumi.Input<ARN>[] = [
    aws.iam.ManagedPolicies.AmazonAthenaFullAccess,
    aws.iam.ManagedPolicies.AmazonS3FullAccess
];

const aggregateTableArgs: BatchInputTableArgs = {
    columns: aggregateTableColumns,
    jobFn: aggregationFunction,
    scheduleExpression,
    policyARNsToAttach,
    dataFormat: "JSON",
}

dataWarehouse.withBatchInputTable(aggregateTableName, aggregateTableArgs);

// create a static fact table

const factTableName = "facts";
const factColumns = [
    {
        name: "thing",
        type: "string"
    },
    {
        name: "color",
        type: "string"
    }
];

const factTableArgs: TableArgs = {
    columns: factColumns,
    dataFormat: "JSON"
};

dataWarehouse.withTable("facts", factTableArgs);

// Load a static facts file into the facts table.
const data = `{"thing": "sky", "color": "blue"}\n{ "thing": "seattle sky", "color": "grey"}\n{ "thing": "oranges", "color": "orange"}`;

const bucketObject = new aws.s3.BucketObject("factsFile", {
    bucket: dataWarehouse.dataWarehouseBucket,
    content: data,
    key: `${factTableName}/facts.json`
});

createEventGenerator("impression", impressionsInputStream.name);
createEventGenerator("clicks", clicksInputStream.name);
