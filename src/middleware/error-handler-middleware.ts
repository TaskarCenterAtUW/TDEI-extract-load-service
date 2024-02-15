import { Request, Response } from 'express';
import HttpException from '../exceptions/http/http-base-exception';

/**
 * A middleware that handles the errors that may occurs in express routes callbacks.
 *
 * This middleware MUST come at the very end of express application middleware pipeline.
 * @param error The error object.
 * @param req The express request instance.
 * @param res The express response instance.
 * @param next The next middleware but actually this should be the last middleware in the pipeline, don't remove this parameter it's important.
 */
export function errorHandler(error: HttpException, req: Request, res: Response): void {
  console.error(error.message, error);
  const status = error.status || 500;
  const message = error.message || 'Something went wrong';
  res
    .status(status)
    .send({
      message,
    });
}
