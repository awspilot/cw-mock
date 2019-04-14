#!/usr/bin/env node





var qs = require('qs')
var AWS = require('aws-sdk')
form_parameters = require('./src/lib/form_parameters')
const DynamodbFactory = require('@awspilot/dynamodb')
DynamodbFactory.config( {empty_string_replace_as: "\0" } );
var CwMock = require("./src/index")

console.log("Starting cloudwatch-mock server on port 10005")

var http = require('http')

http.createServer(function (client_req, client_res) {

	var body = '';
	client_req.on('data', function (data) {body += data;});
	client_req.on('end', function () {

		var auth_re = /(?<algorithm>[A-Z0-9\-]+)\ Credential=(?<accesskey>[^\/]+)\/(?<unknown1>[^\/]+)\/(?<region>[^\/]+)\/([^\/]+)\/([^,]+), SignedHeaders=(?<signed_headers>[^,]+), Signature=(?<signature>[a-z0-9]+)/

		var auth = client_req.headers['authorization'].match( auth_re );
		if (  auth === null )
			return client_res.end('Failed auth');

		//console.log(auth.groups.region )

		var body_json = null
		try {
			body_json = qs.parse(body)
		} catch (err) {
			console.log(err)
		}


		form_parameters.extract_param('Statistics', body_json )
		form_parameters.extract_param('MetricData', body_json )
		console.log(body_json)

		if (body_json.Action === 'PutMetricData') {
			delete body_json.Action;
			delete body_json.Version;

			body_json.MetricData = body_json.MetricData.map(function(md) {
				md.Value = parseFloat(md.Value)
				md.Timestamp = new Date( md.Timestamp )
				return md;
			})


			dbcloudwatch = new CwMock({
				table_name: 'cloudwatch_stats',
				DynamoDB: new DynamodbFactory(
					new AWS.DynamoDB({
						endpoint:        process.env.CW_DYNAMODB_ENDPOINT,
						accessKeyId:     process.env.CW_DYNAMODB_KEY,
						secretAccessKey: process.env.CW_DYNAMODB_SECRET,
						region:          'aws-' + auth.groups.region,
					})
				),
				streams_enabled: false,
			});


 			dbcloudwatch.putMetricData(body_json,function(err,data) {
				console.log( err, data )
				client_res.end('')
			});
			return ;
		}


		console.log("[cloudwatch-mock] received request ",JSON.stringify({
			url: client_req.url,
			hostname: client_req.hostname,
			host: client_req.host,
			port: client_req.port,
			path: client_req.path,
			method: client_req.method,
			headers: client_req.headers,
			//timeout // ms
		}, null, "\t"));

		client_res.end('asd');

		// client_req.headers.host = 'localhost';
		// var proxy_options = {
		// 	host: 'localhost',
		// 	port: 8000,
		// 	path: '/',
		// 	method: client_req.method,
		// 	headers: client_req.headers,
		// }


		// console.log("proxying request to ", JSON.stringify(proxy_options, null,"\t"))

		// var req=http.request(proxy_options, function(res) {
		// 	var body = '';
		// 	res.on('data', function (chunk) {
		// 		body += chunk;
		// 	});
		// 	res.on('end', function () {
		// 		console.log("proxy ended")
		// 		client_res.writeHead(res.statusCode, res.headers);
		// 		client_res.end(body);
		// 	});
		// });
		// req.on('error', function(err) {
		// 	console.log("proxy errored")
		// 	console.log("target error")
		// 	client_res.end('error: ' + err.message);
		// });
		// req.write(body);
		// req.end();



	});
}).listen(10005);
