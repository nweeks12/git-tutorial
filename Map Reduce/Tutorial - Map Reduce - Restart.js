/** * @NApiVersion 2.0 * @NScriptType mapreducescript */
define(['N/search', 'N/record'], function (search, record) {
	return {
		getInputData: function (context) {
			var filter1 = search.createFilter({
				name: 'mainline',
				operator: search.Operator.IS,
				values: true
			});
			var filter2 = search.createFilter({
				name: 'custbody_processed_flag',
				operator: search.Operator.IS,
				values: false
			});
			var column1 = search.createColumn({
				name: 'recordtype'
			});
			var srch = search.create({
				type: search.Type.SALES_ORDER,
				filters: [filter1, filter2],
				columns: [column1]
			});
			return srch;
		},
		map: function (context) {
			var soEntry = JSON.parse(context.value);
			var alreadyProcessed = false;
			if (context.isRestarted) {
				var lookupResult = search.lookupFields({
					type: soEntry.values.recordtype,
					id: context.key,
					columns: ['custbody_processed_flag']
				});
				alreadyProcessed = lookupResult.custbody_processed_flag;
			}
			if (!alreadyProcessed) {
				// UPDATE so FIELDS
				record.submitFields({
					type: soEntry.values.recordtype,
					id: context.key,
					values: {
						custbody_processed_flag: true
					}
				});
			}
			context.write(soEntry.values.recordtype, context.key);
		},
		reduce: function (context) {
			context.write(context.key, context.values.length);
		},
		summarize: function (summary) {
			if (summary.isRestarted) {
				log.audit({
					details: 'Summary stage is being restarted!'
				});
			}
			var totalRecordsUpdated = 0;
			summary.output.iterator().each(function (key, value) {
				log.audit({
					title: key + ' records updated',
					details: value
				});
				totalRecordsUpdated += parseInt(value);
				return true;
			});
			log.audit({
				title: 'Total records updated',
				details: totalRecordsUpdated
			});
		}
	}
});