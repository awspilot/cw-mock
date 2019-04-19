async = require('async')
AWS = require('aws-sdk')

// cloudwatch dynamodb
process.env.CW_DYNAMODB_ENDPOINT="http://localhost:8000"
process.env.CW_DYNAMODB_KEY="myKeyId"
process.env.CW_DYNAMODB_SECRET="secretKey"
process.env.CW_DYNAMODB_REGION="us-east-1"
process.env.CW_DYNAMODB_TABLE ="stats"

Cloudwatch = require("../../src/index")

cloudwatch = new Cloudwatch({
	table_name: 'stats',
});


require('../../cli')

cli_cloudwatch = new AWS.CloudWatch({
	endpoint: 'http://localhost:10005/',
	region: process.env.CW_DYNAMODB_REGION,
	credentials: {
		accessKeyId: process.env.CW_DYNAMODB_KEY,
		secretAccessKey: process.env.CW_DYNAMODB_SECRET,
	}
});


