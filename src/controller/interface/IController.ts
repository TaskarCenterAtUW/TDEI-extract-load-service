import { Router } from "express";

export interface IController {
    router: Router;

    intializeRoutes(): void;
}