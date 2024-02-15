import { NextFunction, Request } from "express";
import express from "express";
import { IController } from "./interface/IController";
import { environment } from "../environment/environment";
import { authenticate } from "../middleware/authenticate-middleware";

class ExtractLoadController implements IController {
    public path = '/api/v1/extract-load';
    public router = express.Router();
    constructor() {
        this.intializeRoutes();
    }

    public intializeRoutes() {
        this.router.get(this.path, authenticate, this.getVersions);
    }

    getVersions = async (request: Request, response: express.Response, next: NextFunction) => {

        response.status(200).send({});
    }

}

const extractLoadController = new ExtractLoadController();
export default extractLoadController;