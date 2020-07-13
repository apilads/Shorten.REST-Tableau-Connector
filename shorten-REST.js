(function () {
    // Create the connector object
    const myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = schemaCallback => {
        const cols = [{
            id: "country",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "os",
            alias: "opreating system",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "createdAt",
            dataType: tableau.dataTypeEnum.datetime
        }, {
            id: "domain",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "aliasId",
            alias: "Alias ID",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "alias",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "destination",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "browser",
            dataType: tableau.dataTypeEnum.string
        }];

        const tableSchema = {
            id: "ShortenREST_clicks",
            alias: "Clicks",
            columns: cols,
            incrementColumnId: "createdAt" // use createdat Column as our incremental value 

        };

        schemaCallback([tableSchema]);
    };

    // Download the data
    myConnector.getData = async (table, doneCallback) => {
        const createdAt = table.incrementValue; // retrive the last createdAt value in the table
        let lastId = tableau.connectionData; // use connectiondata value to get and store the lastId we will got to use it in future refresh.
        let clicksTable = [];
        let data;

        //request the data once if there is no lastId
        if (!lastId) {
            data = await fetchData().then(res => res.json());
            lastId = data.lastId;
            if (createdAt) {
                const clicks = data.clicks
                appendNewDataToArray(clicksTable, createdAt, clicks);

            } else {
                const clicks = data.clicks
                addDataToArray(clicksTable, clicks);
            }
        }

        //request all data until there is no lastId
        while (lastId) {
            data = await fetchData(lastId).then(res => res.json());
            tableau.connectionData = lastId
            lastId = data.lastId;
            if (createdAt) {
                const clicks = data.clicks;
                appendNewDataToArray(clicksTable, createdAt, clicks);

            } else {
                const clicks = data.clicks
                addDataToArray(clicksTable, clicks);
            }
        }

        chunkData(table, clicksTable.reverse());
        // tell tableau that we are done we fetching our data
        doneCallback();
    };

    //append a new data in existing table by using incremental refresh feature
    const appendNewDataToArray = (table, date, data) => {
        const lastClickDate = new Date(date);

        for (let index = 0; index < data.length; index++) {
            let newClickDate = new Date(data[index].createdAt);
            if (lastClickDate.getTime() < newClickDate.getTime()) {
                table.push({
                    "country": data[index].country,
                    "os": data[index].os,
                    "createdAt": new Date(data[index].createdAt).toISOString(),
                    "domain": data[index].domain,
                    "aliasId": data[index].aliasId,
                    "alias": data[index].alias,
                    "destination": data[index].destination,
                    "browser": data[index].browser,
                });
            }
        }
    }

    //add a new data for the first time to clicks table no need to check for incremental refresh
    const addDataToArray = (table, data) => {
        for (let index = 0; index < data.length; index++) {
            table.push({
                "country": data[index].country,
                "os": data[index].os,
                "createdAt": new Date(data[index].createdAt).toISOString(),
                "domain": data[index].domain,
                "aliasId": data[index].aliasId,
                "alias": data[index].alias,
                "destination": data[index].destination,
                "browser": data[index].browser,
            });
        }
    }

    // used to fetch data from the server, I used a public free proxy server to overcome The CORS Limitaion
    const fetchData = async (lastId) => {
        const API_KEY = tableau.password;
        let data;
        if (lastId) {
            data = await fetch('https://cors-anywhere.herokuapp.com/https://api.shorten.rest/clicks?continueFrom=' + lastId, {
                headers: {
                    'x-api-key': API_KEY,
                }
            })
        } else {
            data = await fetch("https://cors-anywhere.herokuapp.com/https://api.shorten.rest/clicks", {
                headers: {
                    'x-api-key': API_KEY,
                }
            })
        }
        return data;
    }

    // used to appendrows to tableau in mangable size. it consider one of the best practice
    const chunkData = (table, clicks) => {
        let index = 0;
        const size = 200;
        while (index < clicks.length) {
            table.appendRows(clicks.slice(index, size + index));
            index += size;
            tableau.reportProgress("Getting row: " + index);
        }
    }

    //chceck if the api key that user provide is valid or not
    const isKeyVaild = async key => {
        if (!key) {
            document.getElementById('authorise-Input').style.border = "1px solid #dc3545";
            document.getElementById('required-field').style.display = "block"
            return false
        } else {
            document.getElementById('required-field').style.display = "none"
            document.getElementById('authorise-Input').style.border = "1px solid #e0e0e0";
        }

        document.getElementById('auth-Button').value = "Authorising...";
        document.getElementById('authorise-Input').style.border = "1px solid #e0e0e0";
        document.getElementById('failed').style.display = "none";

        const status = await fetch("https://cors-anywhere.herokuapp.com/https://api.shorten.rest/clicks", {
            headers: {
                'x-api-key': key,
            }
        }).then(res => {
            if (res.status === 200) {
                document.getElementById('auth-Button').value = "Authorised";
                document.getElementById('authorise-Input').disabled = true;
                document.getElementById('auth-Button').style.display = "none";
                document.getElementById('data-Button').style.display = "block";

            } else {
                document.getElementById('auth-Button').value = "Authorise";
                document.getElementById('authorise-Input').style.border = "1px solid #dc3545";
                document.getElementById('failed').style.display = "block";
            }

            return res.status
        })

        return status === 200 ? true : false;
    }

    tableau.registerConnector(myConnector);

    //add event listeners for when the user submits the authorise button
    document
        .getElementById('auth-Button')
        .addEventListener('click', async (event) => {
            event.preventDefault();
            const apikey = document.getElementById('authorise-Input').value;

            //validate the api Key
            const isValid = await isKeyVaild(apikey);

            //start the web conncoter engine
            if (isValid) {
                // save the api key to use in getdata phase
                tableau.password = apikey

                //add event listeners for when the user submits the get clicks button
                document
                    .getElementById('data-Button')
                    .addEventListener('click', (event) => {
                        event.preventDefault();
                        // This will be the data source name in Tableau
                        tableau.connectionName = "Shorten.REST";
                        tableau.submit();
                    })
            }

        });
})();