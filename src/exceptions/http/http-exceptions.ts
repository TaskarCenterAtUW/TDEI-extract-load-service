import HttpException from "./http-base-exception";

export class DuplicateException extends HttpException {
    constructor(name: string) {
        super(400, `Input with value '${name}' already exists.`);
    }
}

export class UnAuthenticated extends HttpException {
    constructor() {
        super(401, `User not authenticated/authorized to perform this action.`);
    }
}

export class ForeignKeyException extends HttpException {
    constructor(name: string) {
        super(400, `No reference found for the constraint '${name}' in the system.`);
    }
}

export class FileTypeException extends HttpException {
    constructor() {
        super(400, 'Invalid file type.');
    }
}

export class OverlapException extends HttpException {
    constructor(name: string) {
        super(400, `Given record overlaps with tdeirecord ${name} in the system.`);
    }
}

export class UserNotFoundException extends HttpException {
    constructor(name: string) {
        super(404, `User not found for the given username '${name}'.`);
    }
}

export class InputException extends HttpException {
    constructor(message: string) {
        super(400, message);
    }
}

export class JobIdNotFoundException extends HttpException {
    constructor(jobId: string) {
        super(404, `JobId with ID ${jobId} not found`)
    }
}

export class ServiceNotFoundException extends HttpException {
    constructor(serviceId: string) {
        super(404, `Service ID ${serviceId} is not found or inactive`);
    }
}


export class JobIncompleteException extends HttpException {
    constructor(jobId: string) {
        super(404, `JobId with ID ${jobId} not completed`)
    }
}

