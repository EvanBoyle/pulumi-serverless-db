import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import { ServerlessDataWarehouse, StreamingInputTableArgs } from "../../lib/datawarehouse";
import { createEventGenerator } from "./eventGenerator";

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

const impressionsTableName = "impressions";
const clicksTableName = "clicks";

const genericTableArgs: StreamingInputTableArgs = {
    columns,
    inputStreamShardCount: shards[stage],
    region,
    scheduleExpression
}

const dataWarehouse = new ServerlessDataWarehouse("analytics_dw")
    .withStreamingInputTable(impressionsTableName, genericTableArgs)
    .withStreamingInputTable(clicksTableName, genericTableArgs);

const impressionsInputStream = dataWarehouse.getInputStream(impressionsTableName);
const clicksInputStream = dataWarehouse.getInputStream(clicksTableName);

export const impressionInputStream = impressionsInputStream.name;
export const clickInputStream = clicksInputStream.name;

createEventGenerator("impression", impressionsInputStream.name);
createEventGenerator("clicks", clicksInputStream.name);