const {InfluxDB, Point, consoleLogger} =  require('@influxdata/influxdb-client');
const axios = require('axios').default;
const {token, org, url, bucket, endpoint} = require('./env');
const {names, location} = require('./names');

const writeApi = new InfluxDB({url, token}).getWriteApi(org, bucket, 's')
writeApi.useDefaultTags({location: location})

let db = {}

async function getVal() {
    try {
      const response = await axios.get(endpoint);
      if (response.status === 200) {
        return response.data
      } 
    } catch (error) {
      console.error(error);
    }

    return null;
  }

const writeFloat = (key, val, did) => {
    let name = names[`${did}`] || 'Sin Nombre'
    const point1 = new Point(key)
    .tag('did',  did, 'name', name)
    .floatField('value', val)

    writeApi.writePoint(point1);
}

const writeInt = (key, val, did) => {
    let name = names[`${did}`] || 'Sin Nombre'
    const point1 = new Point(key)
    .tag('did',  did, 'name', name)
    .intField('value', val)

    writeApi.writePoint(point1);
}

const saveSensor = async (s) => {
    console.log("Saving sensor");
    console.log(s);

    writeFloat('temperature', s.temp, s.id);
    writeFloat('humi', s.humi, s.id);
    writeInt('co2', s.co2, s.id);
    writeInt('pm1.0', s.pm1, s.id);
    writeInt('pm2.5', s.pm25, s.id);
    writeInt('pm10', s.pm10, s.id);
    writeInt('rssi', s.rssi, s.id);
    writeInt('snr', s.snr, s.id);
    writeInt('counter', s.counter, s.id);

    await writeApi.flush();
}

const fixBadJson = (bad) => {
    return bad.replaceAll('}{', '},{');
}

const insertLocally = (sensor) => {
    const id = `${sensor.id}`;
    let isNew = false;

    if (db[id] == null) {
        console.log('Se inserto sensor nuevo');
        db[id] = sensor;
        return true;
    }

    if (db[id].temp != sensor.temp) isNew = true;
    if (db[id].humi != sensor.humi) isNew = true;
    if (db[id].counter != sensor.counter) isNew = true;
    if (db[id].co2 != sensor.co2) isNew = true;
    if (db[id].pm1 != sensor.pm1) isNew = true;
    if (db[id].pm25 != sensor.pm25) isNew = true;
    if (db[id].pm10 != sensor.pm10) isNew = true;
    if (db[id].rssi != sensor.rssi) isNew = true;
    if (db[id].snr != sensor.snr) isNew = true;

    if (isNew) {
        db[id] = sensor;
    }

    return isNew;
}

const main = async () => {
    let data = await getVal();

    if (data == null) {
        console.error('No se recibio respuesta');
        return;
    }

    if (data.length == 0) {
        console.error('Respuesta no esperada');
        return;
    }
    
    try {
        data = fixBadJson(data);
        data = JSON.parse(data);
        for (let s of data) {
            if (insertLocally(s)) {
                await saveSensor(s);
            }
        }
    } catch (e) {
        console.error(e)
    }


}

setInterval(main, 5000);


