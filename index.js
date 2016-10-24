/**
 * GPS grab and store:
 * @param {Object} tdx Api object.
 * @param {Object} output functions.
 * @param {Object} packageParams of the databot.
 */
function GrabTraffic(tdxApi, output, packageParams) {
    var req = function () {
        return tdxApi.getDatasetDataAsync(packageParams.trafficSources, null, null, null)
            .then((sourcesData) => {
                output.debug("Retrieved Traffic sources table: %d entries", sourcesData.data.length);

                return Promise.all(_.map(sourcesData.data, (sources) => {
                    return request({
                        method: 'GET',
                        url: "http://" + sources.APIKey + ":@" + sources.Host + sources.Path,
                        simple: true
                    })
                        .then(parseXmlStringAsync)
                        .then((result) => {
                            var entry = {};

                            entry['ID'] = sources.ID;
                            _.forEach(result.feed.datastream, (val) => {
                                var valNum = Number(val.current_value[0]);

                                if (val.current_value[0] == 'NULL' || val.current_value[0] == '-1')
                                    valNum = 0;

                                if (val.tag[0] == 'time_stamp')
                                    entry['timestamp'] = valNum;
                                else if (val.tag[0] == 'entry_congestion_level')
                                    entry['EntryCongestionLevel'] = valNum;
                                else if (val.tag[0] == 'exit_congestion_level')
                                    entry['ExitCongestionLevel'] = valNum;
                                else if (val.tag[0] == 'RoundaboutEntry')
                                    entry['RoundaboutEntry'] = valNum;
                                else if (val.tag[0] == 'RoundaboutEntrySpeed')
                                    entry['RoundaboutEntrySpeed'] = valNum;
                                else if (val.tag[0] == 'RoundaboutExit')
                                    entry['RoundaboutExit'] = valNum;
                                else if (val.tag[0] == 'RoundaboutExitSpeed')
                                    entry['RoundaboutExitSpeed'] = valNum;
                                else if (val.tag[0] == 'RoundaboutInside')
                                    entry['RoundaboutInside'] = valNum;
                                else if (val.tag[0] == 'RoundaboutInsideSpeed')
                                    entry['RoundaboutInsideSpeed'] = valNum;
                            });
                            return (entry);
                        }).catch((error) => {
                            output.debug("Error retrieving API for ID: %d [%d]", sources.ID, error.statusCode);
                            return ({});
                        });
                }));
            })
            .then((result) => {
                var entries = [];

                _.forEach(result, function (val) {
                    if (!_.isEmpty(val))
                        entries.push(val);
                });
                output.debug("Processed %d entries", entries.length);
                return tdxApi.updateDatasetDataAsync(packageParams.trafficDataTable, entries, true);
            })
            .catch((err) => {
                output.error("%s", JSON.stringify(err));
                return err;
            });
    }

    var computing = false;
    var timer = setInterval(() => {
        if (!computing) {
            computing = true;
            req().then((result) => {
                output.debug(result);
                computing = false;
            });

        }
    }, packageParams.timerFrequency);

}

/**
 * Main databot entry function:
 * @param {Object} input schema.
 * @param {Object} output functions.
 * @param {Object} context of the databot.
 */
function databot(input, output, context) {
    "use strict"
    output.progress(0);

    var tdxApi = new TDXAPI({
        commandHost: context.commandHost,
        queryHost: context.queryHost,
        accessTokenTTL: context.packageParams.accessTokenTTL
    });

    Promise.promisifyAll(tdxApi);

    tdxApi.authenticate(context.shareKeyId, context.shareKeySecret, function (err, accessToken) {
        if (err) {
            output.error("%s", JSON.stringify(err));
            process.exit(1);
        } else {
            GrabTraffic(tdxApi, output, context.packageParams);
        }
    });
}

var input;
var _ = require('lodash');
var request = require("request-promise");
var Promise = require("bluebird");
var xml2js = require('xml2js');
var TDXAPI = require("nqm-api-tdx");
var parseXmlString;

Promise.promisifyAll(xml2js);
parseXmlStringAsync = xml2js.parseStringAsync;

if (process.env.NODE_ENV == 'test') {
    // Requires nqm-databot-trafficgrab.json file for testing
    input = require('./databot-test.js')(process.argv[2]);
} else {
    // Load the nqm input module for receiving input from the process host.
    input = require("nqm-databot-utils").input;
}

// Read any data passed from the process host. Specify we're expecting JSON data.
input.pipe(databot);