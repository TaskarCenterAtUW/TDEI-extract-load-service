import App from './app';
import dotenv from 'dotenv';
import "reflect-metadata";
import healthController from './controller/health-controller';
import { environment } from './environment/environment';

//Load environment variables
dotenv.config()

const PORT: number = environment.appPort;

const appContext = new App(
    [
        healthController
    ],
    PORT,
);
appContext.listen();
