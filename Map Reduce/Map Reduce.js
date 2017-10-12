require(['N/file'], function (file) {
	var getInputData = function () {
		var dataset = file.load({
			id: '/Data Sets/Most-Recent-Cohorts-Scorecard-Elements.csv'
		});

		return dataset.lines;
	};

	var map = function (context) {
		var row = context.value.split(',');

		log.debug('map: ' + row[0], row[1]);

		context.write(row[5], row[9]);
	};

	var reduce = function (context) {
		var sum = 0;
		for (var i = 0; i < context.values.length; i++) {
			sum += context.values[i];
		}

		var avg = sum / context.values.length;

		log.debug('reduce: ' + context.key, avg);

		context.write(context.key, avg)
	};

	var summarize = function (summary) {
		summary.output.iterator().each(function (key, value) {
			log.debug('summarize: ' + key, value);
			return true;
		})
	};

	return {
		getInputData: getInputData,
		map: map,
		reduce: reduce,
		summarize: summarize
	};
});