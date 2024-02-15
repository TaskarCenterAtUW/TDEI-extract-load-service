import dotenv from 'dotenv';
dotenv.config();
/**
 * Contains all the configurations required for setting up the core project
 * While most of the parameters are optional, appInsights connection is 
 * a required parameter since it is auto imported in the `tdei_logger.ts`
 */
export const environment = {
    appName: process.env.npm_package_name,
    eventBus: {
        connectionString: process.env.QUEUECONNECTION,
        formatterTopic: process.env.FORMATTER_TOPIC,
        dataServiceTopic: process.env.DATASVC_TOPIC,
        validationSubscription: process.env.VALIDATION_SUBSCRIPTION,
        uploadTopic: process.env.UPLOAD_TOPIC,
        confidenceRequestTopic: process.env.CONF_REQ_TOPIC,
        confidenceResponseTopic: process.env.CONF_RES_TOPIC,
        confidenceResponseSubscription: process.env.CONF_RES_SUB,
        formatterSubscription: process.env.FORMATTER_SUBSCRIPTION,
        validationTopic: process.env.VALIDATION_TOPIC
    },
    database: {
        username: process.env.POSTGRES_USER,
        host: process.env.POSTGRES_HOST,
        password: process.env.POSTGRES_PASSWORD,
        database: process.env.POSTGRES_DB,
        ssl: Boolean(process.env.SSL),
        port: parseInt(process.env.POSTGRES_PORT ?? "5432"),
    },
    appPort: parseInt(process.env.PORT ?? "8080"),
    authPermissionUrl: process.env.AUTH_HOST + '/api/v1/hasPermission',
    secretGenerateUrl: process.env.AUTH_HOST + '/api/v1/generateSecret',
    gatewayUrl: process.env.GATEWAY_URL,
    serviceUrl: process.env.USER_MANAGEMENT_HOST + '/api/v1/service',
    userProjectGroupRolesUrl: process.env.USER_MANAGEMENT_HOST + '/api/v1/project-group-roles'
}