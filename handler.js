'use strict';

const axios = require('axios');

const headers = {
    'Authorization': 'Bearer eyJhbGciOiJSUz...',
    'Content-Type': 'application/json'
}

const geocode = (search_array) => {
    //var address = `${search}`;

    var data = JSON.stringify({
        "addresses": search_array
    });
    
    return axios.post('https://gcp-us-east1.api.carto.com/v3/lds/geocoding/batch', 
        data, {
        headers: headers
    })
   .then(function (response) {
        let result_array = []
        response.data.forEach((element, idx) => { 
            result_array.push(
                {
                    geom: { type: 'Point', coordinates: [
                        parseFloat(element.value[0].longitude), 
                        parseFloat(element.value[0].latitude)] },
                    formated_address: element.value[0].formattedAddress,
                    search: search_array[idx]
                }              
            )
         });

        return result_array;       
   })
    .catch(error => ({ error: error.message }));
}

exports.geocode_table = async event => {
    return await Promise.all(event.data.map((row) => geocode(row[1])))
    .then((ret) => (console.log(ret), { 
        statusCode: 200, 
        data: ret.map((v, idx) => [idx, v])
    }))
    .catch(error => ({ statusCode: 500, body: JSON.stringify(error) }));
};