package grails.plugin.awssdk

import com.amazonaws.AmazonWebServiceClient
import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.cloudformation.AmazonCloudFormationAsyncClient
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient
import com.amazonaws.services.cloudfront.AmazonCloudFrontAsyncClient
import com.amazonaws.services.cloudfront.AmazonCloudFrontClient
import com.amazonaws.services.cloudsearch.AmazonCloudSearchAsyncClient
import com.amazonaws.services.cloudsearch.AmazonCloudSearchClient
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.dynamodb.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.ec2.AmazonEC2AsyncClient
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.elasticache.AmazonElastiCacheAsyncClient
import com.amazonaws.services.elasticache.AmazonElastiCacheClient
import com.amazonaws.services.elasticbeanstalk.AWSElasticBeanstalkAsyncClient
import com.amazonaws.services.elasticbeanstalk.AWSElasticBeanstalkClient
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingAsyncClient
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceAsyncClient
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoderAsyncClient
import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoderClient
import com.amazonaws.services.glacier.AmazonGlacierAsyncClient
import com.amazonaws.services.glacier.AmazonGlacierClient
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementAsyncClient
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient
import com.amazonaws.services.importexport.AmazonImportExportAsyncClient
import com.amazonaws.services.importexport.AmazonImportExportClient
import com.amazonaws.services.opsworks.AWSOpsWorksAsyncClient
import com.amazonaws.services.opsworks.AWSOpsWorksClient
import com.amazonaws.services.rds.AmazonRDSAsyncClient
import com.amazonaws.services.rds.AmazonRDSClient
import com.amazonaws.services.redshift.AmazonRedshiftAsyncClient
import com.amazonaws.services.redshift.AmazonRedshiftClient
import com.amazonaws.services.route53.AmazonRoute53AsyncClient
import com.amazonaws.services.route53.AmazonRoute53Client
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.simpledb.AmazonSimpleDBAsyncClient
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceAsyncClient
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient
import com.amazonaws.services.sns.AmazonSNSAsyncClient
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.storagegateway.AWSStorageGatewayAsyncClient
import com.amazonaws.services.storagegateway.AWSStorageGatewayClient

class AmazonWebService {

    static final String DEFAULT_REGION = 'us-east-1'

    def grailsApplication

    private Map asyncClients = [:]
    private Map clients = [:]
    private Map transferManagers = [:]

    private static final services = [
        "autoScaling":              [endpoint: "autoscaling.%s.amazonaws.com", className: "com.amazonaws.services.autoscaling.AmazonAutoScalingClient"],
        "cloudFormation":           [endpoint: "cloudformation.%s.amazonaws.com", className: "com.amazonaws.services.cloudformation.AmazonCloudFormationClient"],
        "cloudFront":               [endpoint: "cloudfront.amazonaws.com", className: "com.amazonaws.services.cloudfront.AmazonCloudFrontClient"],
        "cloudSearch":              [endpoint: "cloudsearch.%s.amazonaws.com", className: "com.amazonaws.services.cloudsearch.AmazonCloudSearchClient"],
        "cloudWatch":               [endpoint: "monitoring.%s.amazonaws.com", className: "com.amazonaws.services.cloudwatch.AmazonCloudWatchClient"],
        "dynamoDB":                 [endpoint: "dynamodb.%s.amazonaws.com", className: "com.amazonaws.services.dynamodb.AmazonDynamoDBClient"],
        "ec2":                      [endpoint: "ec2.%s.amazonaws.com", className: "com.amazonaws.services.ec2.AmazonEC2Client"],
        "elasticBeanstalk":         [endpoint: "elasticbeanstalk.%s.amazonaws.com", className: "com.amazonaws.services.elasticbeanstalk.AWSElasticBeanstalkClient"],
        "elastiCache":              [endpoint: "elasticache.%s.amazonaws.com", className: "com.amazonaws.services.elasticache.AmazonElastiCacheClient"],
        "elasticLoadBalancing":     [endpoint: "elasticloadbalancing.%s.amazonaws.com", className: "com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient"],
        "elasticMapReduce":         [endpoint: "elasticmapreduce.%s.amazonaws.com", className: "com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient"],
        "elasticTranscoder":        [endpoint: "elastictranscoder.%s.amazonaws.com", className: "com.amazonaws.services.elastictranscoder.AmazonElasticTranscoderClient"],
        "glacier":                  [endpoint: "glacier.%s.amazonaws.com", className: "com.amazonaws.services.glacier.AmazonGlacierClient"],
        "iam":                      [endpoint: "iam.amazonaws.com", className: "com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient"],
        "importExport":             [endpoint: "importexport.amazonaws.com", className: "com.amazonaws.services.importexport.AmazonImportExportClient"],
        "opsWorks":                 [endpoint: "opsworks.us-east-1.amazonaws.com", className: "com.amazonaws.services.opsworks.AWSOpsWorksClient"],
        "rds":                      [endpoint: "rds.%s.amazonaws.com", className: "com.amazonaws.services.rds.AmazonRDSClient"],
        "redshift":                 [endpoint: "redshift.us-east-1.amazonaws.com", className: "com.amazonaws.services.redshift.AmazonRedshiftClient"],
        "route53":                  [endpoint: "route53.amazonaws.com", className: "com.amazonaws.services.route53.AmazonRoute53Client"],
        "s3":                       [endpoint: "s3-%s.amazonaws.com", className: "com.amazonaws.services.s3.AmazonS3Client"],
        "sdb":                      [endpoint: "sdb.%s.amazonaws.com", className: "com.amazonaws.services.simpledb.AmazonSimpleDBClient"],
        "ses":                      [endpoint: "email.%s.amazonaws.com", className: "com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient"],
        "swf":                      [endpoint: "swf.%s.amazonaws.com", className: "com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient"],
        "sns":                      [endpoint: "sns.%s.amazonaws.com", className: "com.amazonaws.services.sns.AmazonSNSClient"],
        "sqs":                      [endpoint: "sqs.%s.amazonaws.com", className: "com.amazonaws.services.sqs.AmazonSQSClient"],
        "storageGateway":           [endpoint: "storagegateway.%s.amazonaws.com", className: "com.amazonaws.services.storagegateway.AWSStorageGatewayClient"]
    ]

    TransferManager getTransferManager(String region = '') {
        if (!region) {
            if (awsConfig['s3']?.region) region = awsConfig['s3'].region
            else if (awsConfig?.region) region = awsConfig.region
            else region = DEFAULT_REGION
        }

        if (!transferManagers[region]) {
            transferManagers[region] = new TransferManager(getS3(region))
        }
        transferManagers[region]
    }

    def methodMissing(String name, args) {
        def async = name.endsWith("Async")
        def validMethod = true
        def serviceName = name.replaceAll(/^get/, "").replaceAll(/Async$/, "")
        serviceName = serviceName[0].toLowerCase() + serviceName.substring(1)

        if (serviceName == "s3" && async == true) {
            validMethod = false
        }
        if (!name.startsWith("get")) {
            validMethod = false
        }
        if (!services.containsKey(serviceName)) {
            validMethod = false
        }

        if (validMethod) {
            AmazonWebService.metaClass."$name" = { Object[] varArgs ->
                getServiceClient(serviceName, varArgs.length > 0 ? varArgs[0] : '', async)
            }
            getServiceClient(serviceName, args.length > 0 ? args[0] : '', async)
        } else {
            throw new MissingMethodException(name, this.class, args)
        }
    }

    // PRIVATE

    private def getAwsConfig() {
        grailsApplication.config.grails?.plugin?.awssdk
    }

    private buildCredentials(defaultConfig, serviceConfig) {
        Map config = [
                accessKey: defaultConfig.accessKey ?: '',
                secretKey: defaultConfig.secretKey ?: ''
        ]
        if (serviceConfig) {
            if (serviceConfig.accessKey) config.accessKey = serviceConfig.accessKey
            if (serviceConfig.secretKey) config.secretKey = serviceConfig.secretKey
        }

        BasicAWSCredentials credentials = new BasicAWSCredentials(config.accessKey, config.secretKey)

        if(!credentials.AWSAccessKeyId || !credentials.AWSSecretKey) {
            return new DefaultAWSCredentialsProviderChain()
        }

        credentials
    }

    private ClientConfiguration buildClientConfiguration(defaultConfig, serviceConfig) {
        Map config = [
                connectionTimeout: defaultConfig.connectionTimeout ?: 0,
                maxConnections: defaultConfig.maxConnections ?: 0,
                maxErrorRetry: defaultConfig.maxErrorRetry ?: 0,
                protocol: defaultConfig.protocol ?: '',
                socketTimeout: defaultConfig.socketTimeout ?: 0,
                userAgent: defaultConfig.userAgent ?: ''
        ]
        if (serviceConfig) {
            if (serviceConfig.connectionTimeout) config.connectionTimeout = serviceConfig.connectionTimeout
            if (serviceConfig.maxConnections) config.maxConnections = serviceConfig.maxConnections
            if (serviceConfig.maxErrorRetry) config.maxErrorRetry = serviceConfig.maxErrorRetry
            if (serviceConfig.protocol) config.protocol = serviceConfig.protocol
            if (serviceConfig.socketTimeout) config.socketTimeout = serviceConfig.socketTimeout
            if (serviceConfig.userAgent) config.connectionTimeout = serviceConfig.userAgent
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration()
        if (config.connectionTimeout) clientConfiguration.connectionTimeout = config.connectionTimeout
        if (config.maxConnections) clientConfiguration.maxConnections = config.maxConnections
        if (config.maxErrorRetry) clientConfiguration.maxErrorRetry = config.maxErrorRetry
        if (config.protocol) {
            if (config.protocol.toUpperCase() == 'HTTP') clientConfiguration.protocol = Protocol.HTTP
            else clientConfiguration.protocol = Protocol.HTTPS
        }
        if (config.socketTimeout) clientConfiguration.socketTimeout = config.socketTimeout
        if (config.userAgent) clientConfiguration.userAgent = config.userAgent
        clientConfiguration
    }

    private AmazonWebServiceClient getServiceClient(String service, String region = '', Boolean async = false) {
        def serviceConfig = services[service]
        def className = serviceConfig.className
        def endpoint = serviceConfig.endpoint

        if (async) {
           className = className.replaceAll(/Client$/, 'AsyncClient')
        }

        if (!region) {
            if (awsConfig[service]?.region) region = awsConfig[service].region
            else if (awsConfig?.region) region = awsConfig.region
            else region = DEFAULT_REGION
        }

        def clientsCache = async ? asyncClients : clients
        if (!clientsCache[service]) clientsCache[service] = [:]

        if (!clientsCache[service].containsKey(region)) {
            AmazonWebServiceClient client
            def credentials = buildCredentials(awsConfig, awsConfig[service])

            ClientConfiguration configuration = buildClientConfiguration(awsConfig, awsConfig[service])

            client = Class.forName(className).newInstance(credentials)
            client.endpoint = getClientEndpoint(service, endpoint, region)
            client.configuration = configuration

            clientsCache[service][region] = client
        } else {
            clientsCache[service][region]
        }
    }

    private String getClientEndpoint(service, endpoint, region) {
        switch(service) {
            case 'S3':
                if (region == 'us' || region == DEFAULT_REGION) {
                    endpoint = "s3.amazonaws.com"
                }
                break
            case ["SdbAsync", "Sdb"]:
                if (region == 'us-east-1') {
                    endpoint = "sdb.amazonaws.com"
                }
                break
        }
        
        String.format(endpoint, region)
    }
}
