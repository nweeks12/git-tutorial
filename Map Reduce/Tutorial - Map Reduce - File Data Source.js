/**
 * This script demonstrates how to use Map/Reduce with a file as the data source. The file comes from https://catalog.data.gov/dataset?res_format=CSV.
 * 
 * @NScriptType MapReduceScript
 * @NApiVersion 2.x
 */
define(['N/file'], function (file) {
	var getInputData = function () {
		try {
			log.debug('start', 'getInputData');
			var dataset = file.load({
				id: '/Data Sets/Most-Recent-Cohorts-Scorecard-Elements.csv'
			});
			log.debug('file loaded', dataset.name);
			log.debug('file size', dataset.size);

			var lines = [];
			var iterator = dataset.lines.iterator();
			iterator.each(function () { return false; });
			iterator.each(function (line) {
				lines.push(line);
				return true;
			});

			log.debug('lines', lines.length);

			return lines;
		} catch (e) {
			log.error('getInputData', JSON.stringify(e));
		}
	};

	var map = function (context) {
		log.debug('map: context.key', context.key);
		try {
			var row = JSON.parse(context.value).value.split(',');

			log.debug('map: ' + row[0], row[3]);
			log.debug('map: SATVR25', row[22]);
			if (row[22] != "NULL") context.write(row[5], parseInt(row[22]));
		} catch (e) {
			log.error('map', JSON.stringify(e));
		}
	};

	var reduce = function (context) {
		try {
			var sum = context.values.reduce(function (sum, num) { return parseInt(sum) + parseInt(num); });

			log.audit('reduce (sum): ' + context.key, sum);

			var avg = sum / context.values.length;

			log.debug('reduce (avg): ' + context.key, avg);

			context.write(context.key, avg);
		} catch (e) {
			log.error('reduce', JSON.stringify(e));
		}
	};

	var summarize = function (summary) {
		try {
			summary.output.iterator().each(function (key, value) {
				log.debug('summarize: ' + key, value);
				return true;
			});
		} catch (e) {
			log.error('summarize', JSON.stringify(e));
		}
	};

	return {
		getInputData: getInputData,
		map: map,
		reduce: reduce,
		summarize: summarize
	};
});