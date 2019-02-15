var async = require('async')
const AWS = require('aws-sdk')
const DynamodbFactory = require('@awspilot/dynamodb')
DynamodbFactory.config( {empty_string_replace_as: "\0" } );

var DynamoDB;

if (process.env.CW_DYNAMODB_ENDPOINT) {
	DynamoDB = new DynamodbFactory(
		new AWS.DynamoDB({
			endpoint:        process.env.CW_DYNAMODB_ENDPOINT,
			accessKeyId:     process.env.CW_DYNAMODB_KEY,
			secretAccessKey: process.env.CW_DYNAMODB_SECRET,
			region:          process.env.CW_DYNAMODB_REGION,
		})
	)
} else {
	if (process.env.CW_DYNAMODB_KEY) {
		DynamoDB = new DynamodbFactory(
			new AWS.DynamoDB({
				accessKeyId:     process.env.CW_DYNAMODB_KEY,
				secretAccessKey: process.env.CW_DYNAMODB_SECRET,
				region:          process.env.CW_DYNAMODB_REGION,
			})
		)
	} else {
		DynamoDB = new DynamodbFactory(
			new AWS.DynamoDB()
		)
	}
}


var Cloudwatch = function( config ) {
	this.config = typeof config === "object" ? config : {}
	
	
}

Cloudwatch.prototype.putMetricData = function( params, cb ) {
	var $this = this;

	var namespace='_'
	if (params.Namespace)
		namespace = params.Namespace
		
	if (this.config.namespace_prefix)
		namespace = this.config.namespace_prefix + namespace

	if (!Array.isArray( params.MetricData ))
		return cb({ errorCode: 'invalid MetricData'})


	async.each(params.MetricData, function(metric, cb ) {

		var payload = {
			namespace: namespace,
			date: 'S ' + new Date(metric.Timestamp).toISOString().substr(0,19).split('T').join(' '),
			expire_at: Math.round(new Date().getTime() / 1000) + (60*60*3) // expire in 3 thours
		}
		
		payload[ metric.MetricName  ] = DynamoDB.add( metric.Value )

		DynamoDB
			.table( $this.config.table_name || process.env.CW_DYNAMODB_TABLE)
			.insert_or_update(payload, function(err) {
				console.log(err ? '☐' : '☑', "increment ", err )
				cb(err)

			})
	}, function(err) {
		cb(err)
	})



	
}
Cloudwatch.prototype.getMetricStatistics = function( params, cb ) {
	cb()
}


module.exports = Cloudwatch;
