describe('all', function () {
	it('CreateStack', function(done) {
		var params = {
			MetricData:[{
				MetricName: 'sent',
				Timestamp:  new Date,
				Value: 1,
			}],
			Namespace: 'hello/world',
		};
		cloudwatch.putMetricData(params,function(err,data) {
			console.log(err ? '☐' : '☑', "putMetricData.sent", err )
			done()
		});
	})
})