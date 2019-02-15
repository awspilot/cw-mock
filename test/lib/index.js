AWS = require('aws-sdk')

Cloudwatch = require("../../src/index")


cloudwatch = new Cloudwatch({
});


// cloudwatch dynamodb
process.env.CW_DYNAMODB_ENDPOINT="http://localhost:8000"
process.env.CW_DYNAMODB_KEY="myKeyId"
process.env.CW_DYNAMODB_SECRET="secretKey"
process.env.CW_DYNAMODB_REGION="us-east-1"

