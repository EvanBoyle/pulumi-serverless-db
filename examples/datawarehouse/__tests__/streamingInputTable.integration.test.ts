import { spawn, ChildProcess } from 'child_process';
jest.setTimeout(360000);

beforeAll(done => {
    const stackCreate = spawn('pulumi', ["stack", "init", "serverless-db.integration.test", '--cwd', './examples/datawarehouse']);
    const onStackCreateComplete = (exitCode: number) => {
        // TODO: make stack name unique (append some randomness)
        // TODO: pass config via --config stringArray
        // expect(exitCode).toEqual(0);
        const stackSelect = spawn('pulumi', ["stack", "select", "serverless-db.integration.test", '--cwd', './examples/datawarehouse']);
        monitorProcAndInitNext(stackSelect, onStackSelectComplete);
    };

    monitorProcAndInitNext(stackCreate, onStackCreateComplete);

    const onStackSelectComplete = (exitCode: number) => {
        expect(exitCode).toBe(0);
        const pulumiUp = spawn('pulumi', ['up', '--non-interactive', '--cwd', './examples/datawarehouse']);
        monitorProcAndInitNext(pulumiUp, onUpComplete);
    };

    const onUpComplete = (exitCode: number) => {
        expect(exitCode).toBe(0)
        done()
    };

});

afterAll(done => {
    const destroy = spawn('pulumi', ['destroy', '--non-interactive', '--cwd', './examples/datawarehouse']);
    const onDestroyComplete = (exitCode: number) => {
        expect(exitCode).toBe(0);
        const stackRm = spawn('pulumi', ["stack", "rm", "serverless-db.integration.test", '--cwd', './examples/datawarehouse', '--yes']);
        monitorProcAndInitNext(stackRm, onRmComplete);
    };
    monitorProcAndInitNext(destroy, onDestroyComplete);

    const onRmComplete = (exitCode: number) => {
        expect(exitCode).toBe(0);
        done()
    };

});

test("WithStreamingInput integrtion test", done => {
    // TODO: try to read one row from each table, with exponential backoff as it will take a minute or two for data to appear in athena
    expect(true).toBe(true); // placeholder
    done()
});

const monitorProcAndInitNext = (proc: ChildProcess, onComplete: (exitCode: number)=>any) => {
    proc.stdout.setEncoding('utf8');
    proc.stderr.setEncoding('utf8');

    proc.stdout.on('data', (chunk) => {
        console.log(chunk)
        // data from standard output is here as buffers
    });

    proc.stderr.on('data', (chunk) => {
        console.log(chunk)
    });

    proc.on('close', (code) => {
        console.log(`child process exited with code ${code}`);
        onComplete(code);
    });
}