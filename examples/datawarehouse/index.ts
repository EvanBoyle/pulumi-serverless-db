import * as pulumi from "@pulumi/pulumi";

import { ServerlessDataWarehouse, StreamingInputTableArgs } from "../../lib/datawarehouse";
import { createEventGenerator } from "./eventGenerator";

// app specific config
const config = new pulumi.Config();
const awsConfig = new pulumi.Config("aws")
const region = awsConfig.require("region");
const stage = config.require("stage");
const shards: { [key: string]: number } = config.requireObject("shards");
const isDev = config.get("dev") === 'true';
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

const impressionsTableName = "impressions";
const clicksTableName = "clicks";

const genericTableArgs: StreamingInputTableArgs = {
    columns,
    inputStreamShardCount: shards[stage],
    region,
    scheduleExpression
}

const dataWarehouse = new ServerlessDataWarehouse("analytics_dw", { isDev })
    .withStreamingInputTable(impressionsTableName, genericTableArgs)
    .withStreamingInputTable(clicksTableName, genericTableArgs);

const impressionsInputStream = dataWarehouse.getInputStream(impressionsTableName);
const clicksInputStream = dataWarehouse.getInputStream(clicksTableName);

createEventGenerator("impression", impressionsInputStream.name);
createEventGenerator("clicks", clicksInputStream.name);

export const impressionInputStream = impressionsInputStream.name;
export const clickInputStream = clicksInputStream.name;
export const databaseName = dataWarehouse.database.name;
export const impressionTableName = dataWarehouse.getTable(impressionsTableName).name;
export const clickTableName = dataWarehouse.getTable(clicksTableName).name;
export const athenaResultsBucket = dataWarehouse.queryResultsBucket.bucket;
