export default class UniqueKeyDbException extends Error {
    constructor(message: string) {
        super(message);
    }
}

export class ForeignKeyDbException extends Error {
    constructor(message: string) {
        super(message);
    }
}