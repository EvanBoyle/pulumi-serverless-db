import { resolve } from "path";
import * as athena from "athena-client"

import { PulumiRunner } from "../../../testing/integration";

jest.setTimeout(360000);

let runner: PulumiRunner;
const region = "us-west-2";

beforeAll(async () => {
    const config: { [key: string]: string } = {
        "aws:region": region,
        "pulumi-serverless-db:shards": '{"prod": 8, "gamma": 4, "beta": 1}',
        "pulumi-serverless-db:stage": "beta",
        "pulumi-serverless-db:dev": "true"
    };

    const pulumiProjDir = resolve("./examples/datawarehouse");
    runner = new PulumiRunner(config, pulumiProjDir);
    const setupResult = await runner.setup();
    if(!setupResult.success) {
        throw new Error(`Pulumi setup failed, aborting: ${setupResult.error}`)
    }
});

afterAll(async () => {
    const teardownResult = await runner.teardown();
    if(!teardownResult.success) {
        throw new Error(`Pulumi teardown failed. Test stack has leaked: ${teardownResult.error}`)
    }
});

test("WithStreamingInput integrtion test", async () => {
    expect(runner.getStackOutputKeys().length).toBe(6);
    const db = runner.getStackOutput("databaseName");
    const clickTable = runner.getStackOutput("clickTableName");
    const impressionTable = runner.getStackOutput("clickTableName");
    const bucket = runner.getStackOutput("athenaResultsBucket");

    const clickPromise = verifyRecordsInTable(db, clickTable, bucket);
    const impressionPromise = verifyRecordsInTable(db, impressionTable, bucket);

    const [clickTableHasRecords, impressionTableHasRecords] = await Promise.all([clickPromise, impressionPromise]);

    expect(clickTableHasRecords).toBe(true);
    expect(impressionTableHasRecords).toBe(true);
});

const verifyRecordsInTable = async (db: string, table: string, bucket: string) => {
    const bucketUri = `s3://${bucket}`;
    const clientConfig = {
        bucketUri
    };
    const awsConfig = {
        region
    };
    const athenaClient = athena.createClient(clientConfig, awsConfig);

    let didFindResults = false;
    const query = `select * from ${db}.${table} limit 10;`
    let retry = 0;
    while(retry < 4) {
        const result = await athenaClient.execute(query).toPromise();
        if(result.records.length > 0) {
            didFindResults = true;
            break;
        }
        else {
            retry++;
            await new Promise(resolve => setTimeout(resolve, 30000));
        }
    }

    return didFindResults
}
