
import express, { NextFunction, Request, Response } from "express";
import bodyParser from "body-parser";
import { IController } from "./controller/interface/IController";
import helmet from "helmet";
import { Core } from "nodets-ms-core";
import { unhandledExceptionAndRejectionHandler } from "./middleware/unhandled-exception-rejection-handler";
import dbClient from "./database/data-source";
import HttpException from "./exceptions/http/http-base-exception";
import { processMiddleware } from "./middleware/app-context-middleware";
import subscriptions from "./subscriptions.json";
import { QueueService } from "./service/queue-service";

class App {
    private app: express.Application;
    private port: number;
    private queueService: QueueService = new QueueService({});

    constructor(controllers: IController[], port: number) {
        this.app = express();
        this.port = port;
        //First middleware to be registered: after express init
        unhandledExceptionAndRejectionHandler();

        this.initializeMiddlewares();
        this.initializeControllers(controllers);
        this.initializeLibraries();
        dbClient.initializaDatabase();
        this.initializeQueueSubscriptions();
        //Last middleware to be registered: error handler. 
        this.app.use((err: any, req: Request, res: Response, next: NextFunction) => {
            console.log(err);
            if (err instanceof HttpException) {
                res.status(err.status).send(err.message);
            }
            else {
                res.status(500).send('Application error occured');
            }
        });
    }

    private initializeQueueSubscriptions() {
        this.queueService = new QueueService(subscriptions);
        this.queueService.initializeQueue();
    }

    private initializeLibraries() {
        Core.initialize();
    }

    private initializeMiddlewares() {
        this.app.use(helmet());
        this.app.use(bodyParser.json());
    }

    private initializeControllers(controllers: IController[]) {
        this.app.use("/", processMiddleware);
        controllers.forEach((controller) => {
            this.app.use(controller.router);
        });
    }

    public listen() {
        this.app.listen(this.port, () => {
            console.log(`App listening on the port ${this.port}`);
        });
    }
}

export default App;
