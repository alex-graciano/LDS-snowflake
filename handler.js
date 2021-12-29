'use strict';

const axios = require('axios');

let headers = {
    'Content-Type': 'application/json'
}

const getGeocodePoint = (cloud, longitude, latitude) => {
    let geom;
    switch(cloud) {
      case 'snowflake':
        geom = { 
            type: 'Point', 
            coordinates: [
                parseFloat(longitude), 
                parseFloat(latitude)
            ] 
        }
        break;
      case 'redshift':
        geom = 'POINT (' + parseFloat(longitude) + ' ' + parseFloat(latitude) + ')'
        break;
    }
    
    return geom;
}

const geocode = (search_array, cloud, token) => {
    let addresses = [];
    
    switch(cloud)
    {
      case 'snowflake':
        addresses = search_array;
        break;
      case 'redshift':
        addresses = JSON.parse(search_array);
        break;
    }

    const data = JSON.stringify({
        "addresses": addresses
    });
    
    headers.Authorization = token;
    
    return axios.post('https://gcp-us-east1.api.carto.com/v3/lds/geocoding/batch', 
        data, {
        headers: headers
    })
   .then(function (response) {
        let result_array = []
        response.data.forEach((element, idx) => {
            result_array.push(
                {
                    geom: getGeocodePoint(cloud, element.value[0].longitude, element.value[0].latitude),
                    search: addresses[idx]
                }              
            )
         });

        return result_array;       
   })
    .catch(error => ({ error: error.message }));
}

const getInputObject = (event, cloud) => {
    let inputObject = {};
    
    switch(cloud) {
      case 'snowflake':
        inputObject.inputArray = event.data;
        inputObject.indexArray = 3;
        inputObject.token = 'Bearer ' + event.data[0][2];
        break;
      case 'redshift':
        inputObject.inputArray = event.arguments;
        inputObject.indexArray = 2;
        inputObject.token = 'Bearer ' + event.arguments[0][1];
        break;
    }
    
    return inputObject;
}

const getOutputObject = (response, cloud) => {
    let outputObject = {success: true};
    
    switch(cloud) {
      case 'snowflake':
        outputObject.data = response.map((v, idx) => [idx, v]);
        break;
      case 'redshift':
        outputObject.results = [JSON.stringify(response)];
        outputObject = JSON.stringify(outputObject);
        break;
    }
    
    return outputObject;
}

const getError = (error, cloud) => {
    let outputObject = {success: false};
    
    switch(cloud) {
      case 'snowflake':
        //outputObject.data = [[0, JSON.stringify(error)]];
        outputObject.data = [[0, "Ha dao un error"]];
        break;
      case 'redshift':
        outputObject.results = [JSON.stringify(error)];
        outputObject.error_msg = JSON.stringify(error);
        break;
    }
    
    return outputObject;
}

const getCloud = (event) => {
  if (event.hasOwnProperty('data'))
    return event.data[0][1];
  if (event.hasOwnProperty('arguments'))
    return event.arguments[0][0];
}

exports.geocode_table = async event => {
    // Token checking

    // Cloud checking
    const cloud = getCloud(event);
    const input = getInputObject(event, cloud);

    // API call
    return await Promise.all(input.inputArray.map((row) => 
        geocode(row[input.indexArray], cloud, input.token)))
    .then(function (response) {
        
        return getOutputObject(response, cloud);
    })
    .catch(function (error) { 
        
        return getError(error, cloud);
    });
};