/**
 * Middleware to handle the token authorization etc.
 */

import { Request, Response, NextFunction } from 'express';
import { Core } from 'nodets-ms-core';
import { environment } from "../environment/environment";
import { UnAuthenticated } from '../exceptions/http/http-exceptions';
import tdeiService from '../service/tdei-service';
import { PermissionRequest } from 'nodets-ms-core/lib/core/auth/model/permission_request';
import HttpException from '../exceptions/http/http-base-exception';

/**
 * Authorizes the request with provided allowed roles and tdei_project_group_id
 * the user id is available as `req.user_id`
 * @param req - Initial request
 * @param res  - Supposed response (to be filled by others)
 * @param next - Next function
 */
export const authorize = (approvedRoles: string[]) => {
    return async (req: Request, res: Response, next: NextFunction) => {
        console.log("authorize middleware");
        if (!req.body.user_id)
            return next(new UnAuthenticated());

        if (req.params["tdei_record_id"]) {
            //Fetch tdei_project_group_id from tdei_record_id
            try {
                let tdei = await tdeiService.getRecordById(req.params["tdei_record_id"]);
                req.body.tdei_project_group_id = tdei.tdei_project_group_id;
            } catch (error) {
                if (error instanceof HttpException) {
                    return next(error);
                }
                return next(new HttpException(500, "Error processing the request"));
            }
        }
        else if (req.params["tdei_project_group_id"]) {
            req.body.tdei_project_group_id = req.params["tdei_project_group_id"];
        }
        else {
            console.error("authorize:tdei_project_group_id cannot be extracted");
            return next(new Error("authorize:tdei_project_group_id cannot be extracted"));
        }

        //If no roles skip the check
        if (!approvedRoles.length)
            return next();

        const authProvider = Core.getAuthorizer({ provider: "Hosted", apiUrl: environment.authPermissionUrl });
        const permissionRequest = new PermissionRequest({
            userId: req.body.user_id as string,
            projectGroupId: req.body.tdei_project_group_id,
            permssions: approvedRoles,
            shouldSatisfyAll: false
        });

        try {
            const response = await authProvider?.hasPermission(permissionRequest);
            if (response) {
                next();
            }
            else {
                next(new UnAuthenticated());
            }
        }
        catch (error) {
            next(new UnAuthenticated());
        }
    }
}