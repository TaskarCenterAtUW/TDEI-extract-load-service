import express, { Request } from "express";
import { IController } from "./interface/IController";

class HealthController implements IController {
    public path = '/';
    public router = express.Router();

    constructor() {
        this.intializeRoutes();
    }

    public intializeRoutes() {
        this.router.get(`${this.path}health/ping`, this.getping);
        this.router.post(`${this.path}health/ping`, this.getping);
        this.router.get(`${this.path}`, this.getping);
    }

    public getping = async (request: Request, response: express.Response) => {
        // return loaded posts
        response.status(200).send("I'm healthy !!");
    }
}

const healthController = new HealthController();
export default healthController;