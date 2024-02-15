import * as http from 'http';
import { ServerStatus } from '../constants/server-status-constants';
const { setTimeout: sleep } = require('node:timers/promises');

class AppContext {

    public applicationStatus: ServerStatus = ServerStatus.Initializing;
    private processCount = 0;
    public server: http.Server<typeof http.IncomingMessage, typeof http.ServerResponse> | undefined;


    SHUTDOWN_SIGNALS: string[] = ['SIGINT', 'SIGTERM'];
    SHUTDOWN_TIMEOUT: number = 30000; //30s
    TERMINATION_CHECK_SLEEP_MILLIS = 1000; //1s

    processOnce = (signals: string[], fn: any) => {
        return signals.forEach(sig => process.once(sig, fn));
    };

    forceExitAfter = (timeout: number) => () => {
        setTimeout(() => {
            // Force shutdown after timeout
            console.warn(`Could not close resources gracefully after ${timeout}ms: forcing shutdown..`);
            return process.exit(1);
        }, timeout).unref();
    };

    shutdownHandler = async (signalOrEvent: string) => {
        console.warn(`Shutting down: received [${signalOrEvent}] signal`);
        appContext.applicationStatus = ServerStatus.Stopping;
        while (appContext.tasksCount != 0) {
            console.log("Waiting for " + appContext.tasksCount + " request to complete before shutting down..");
            await sleep(this.TERMINATION_CHECK_SLEEP_MILLIS);
        }

        console.log('Starting shutdown of application..');
        this.server!.close(function () {
            console.log('Application gracefully shutdown.');
            process.exit(0);
            //TODO: restart scenario
        });
    }

    initialize(server: http.Server<typeof http.IncomingMessage, typeof http.ServerResponse>) {
        this.server = server;
        this.processOnce(this.SHUTDOWN_SIGNALS, this.forceExitAfter(this.SHUTDOWN_TIMEOUT));
        //Parellel process to spawn in the case of forceful shutdown
        this.processOnce(this.SHUTDOWN_SIGNALS, this.shutdownHandler);
    }

    public get tasksCount() { return this.processCount; }
    //Every process call this method before starting the process
    public processStart() {
        if (this.applicationStatus == ServerStatus.Running) {
            this.processCount++;
            return true;
        }
        return false;
    }
    //Every process call this method after finishing the process
    public processEnd() {
        if (this.processCount > 0) {
            this.processCount--;
            return true;
        }
        return false;
    }
}

const appContext = new AppContext();
export default appContext;
