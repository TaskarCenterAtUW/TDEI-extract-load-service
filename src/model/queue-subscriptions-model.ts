export class QueueConfig {

    constructor(config: Partial<QueueConfig>) {
        Object.assign(this, config);
    }

    subscriptions: Subscription[] = [];
}

export interface Subscription {
    description: string;
    topic: string;
    subscription: string;
}