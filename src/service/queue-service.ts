import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { QueueConfig } from "../model/queue-subscriptions-model";
import { Topic } from "nodets-ms-core/lib/core/queue/topic";
import { Core } from "nodets-ms-core";
import extractLoadService from "./extract-load-service";
import { environment } from "../environment/environment";

export class QueueService {

    private queueConfig: QueueConfig = new QueueConfig({});

    constructor(public config: any) { }
    private topicCollection = new Map<string, Topic>();

    initializeQueue() {
        console.log('Queue initialized');
        this.queueConfig = new QueueConfig(this.config);
        this.subscribe();
    }

    /**
   * Method to to get the topic instance by name
   * @param topicName 
   * @returns 
   */
    private getTopicInstance(topicName: string) {
        let topic = this.topicCollection.get(topicName);
        if (!topic) {
            topic = Core.getTopic(topicName, null, environment.eventBus.maxConcurrentMessages);
            this.topicCollection.set(topicName, topic);
        }
        return topic;
    }

    /**
    * Subscribe all
    */
    private subscribe() {
        console.log("Subscribing TDEI subscriptions");
        this.queueConfig.subscriptions.forEach(subscription => {
            var topic = this.getTopicInstance(subscription.topic as string);
            topic.subscribe(subscription.subscription as string,
                {
                    onReceive: this.handleMessage,
                    onError: this.handleFailedMessages
                });
        });
    }

    /** 
     * Handle the subscribed messages
     * @param message 
     */
    private handleMessage = async (message: QueueMessage) => {
        try {
            await extractLoadService.extractLoadRequestProcessor(message);
        } catch (error) {
            console.error("Error in handling incoming message", error);
        }
        return Promise.resolve();
    }

    /**
    * Handles the failure message handling
    * @param error 
    */
    private handleFailedMessages(error: Error) {
        console.log('Error handling the message');
        console.log(error);
    }
}