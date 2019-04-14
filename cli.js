#!/usr/bin/env node

var qs = require('qs')
form_parameters = require('./src/lib/form_parameters')
var CwMock = require("./src/index")
dbcloudwatch = new CwMock({
	table_name: 'cloudwatch_stats',
	dynamic_regions: true,
});

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
		console.log(body_json)

		//if (body_json.Action === '')


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
