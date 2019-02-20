describe('putMetricData', function () {
	it('sleep', function(done) {
		setTimeout(done, 4000)
	})
	it('putMetricData', function(done) {
		var params = {
			MetricData:[{
				MetricName: 'sent',
				Timestamp:  new Date,
				Value: Math.floor(Math.random()*1000),
			}],
			Namespace: 'hello/world/1',
		};
		cloudwatch.putMetricData(params,function(err,data) {
			console.log(err ? '☐' : '☑', "putMetricData.sent", err )
			done()
		});
	})
})