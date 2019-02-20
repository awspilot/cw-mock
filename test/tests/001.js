describe('putMetricData', function () {
	it('sleep', function(done) {
		setTimeout(done, 4000)
	})
	it('putMetricData', function(done) {
		var params = {
			MetricData:[{
				MetricName: 'sent',
				Timestamp:  new Date,
				Value: 1,
			}],
			Namespace: 'hello/world/1',
		};
		cloudwatch.putMetricData(params,function(err,data) {
			console.log(err ? '☐' : '☑', "putMetricData.sent", err )
			done()
		});
	})
})