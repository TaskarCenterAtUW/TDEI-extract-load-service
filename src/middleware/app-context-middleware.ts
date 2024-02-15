import { Request, Response, NextFunction } from "express";
import appContext from "../model/app-context";
import { ServerStatus } from "../constants/server-status-constants";

export const processMiddleware = function (req: Request, res: Response, next: NextFunction) {
    if (appContext.applicationStatus == ServerStatus.Stopping) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        return res.end('Server is shutting down.');
    }

    if (appContext.applicationStatus != ServerStatus.Running) {
        res.writeHead(503, { 'Content-Type': 'text/plain' });
        return res.end('Server is not available. Please try after sometime.');
    }

    if (appContext.processStart()) {
        console.debug('Starting request ' + appContext.tasksCount);
    }

    res.on('finish', async () => {
        if (appContext.processEnd())
            console.debug('Finishing request ' + (Number.parseInt(appContext.tasksCount.toString()) + 1));
    });
    next();
}