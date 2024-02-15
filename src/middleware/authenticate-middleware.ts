/**
 * Middleware to handle the token authorization etc.
 */

import { Request, Response, NextFunction } from 'express';
import jwt, { JwtPayload } from 'jsonwebtoken';
import { UnAuthenticated } from '../exceptions/http/http-exceptions';

/**
 * Validates the token and sends the user_id in the req.body
 * the user id is available as `req.user_id`
 * @param req - Initial request
 * @param res  - Supposed response (to be filled by others)
 * @param next - Next function
 */

export async function authenticate(req: Request, res: Response, next: NextFunction) {

    // Get the authorization key
    const bearerHeader = req.headers.authorization;
    if (bearerHeader === '' || bearerHeader === undefined) {
        // res.status(401).send('Unauthorized');
        next(new UnAuthenticated());
    }
    else {
        // Get the bearer
        const bearer = bearerHeader!.replace(/^Bearer\s/, '');
        if (bearer === '' || bearer === undefined) {
            next(new UnAuthenticated());
            return
        }
        // Decode the token
        const jwtOutput = jwt.decode(bearer) as JwtPayload;
        if (jwtOutput == null) {
            next(new UnAuthenticated());
            return
        }
        req.body.user_id = jwtOutput?.sub;
        req.body.isAdmin = jwtOutput?.realm_access?.roles.includes('tdei-admin');
        next();
    }
}