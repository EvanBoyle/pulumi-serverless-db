import * as moment from "moment-timezone";

export const createPartitionDDLStatement=(dbName:string,locationPath:string,eventTime:string,hours = 12): string =>{
    let date = moment(eventTime);
        
    let query = `ALTER TABLE ${dbName}.logs ADD IF NOT EXISTS`;

    for(let i = 0; i <= hours; i++) {
        const dateString = date.utc().format("YYYY/MM/DD/HH");

        query += `\nPARTITION (inserted_at = '${dateString}') LOCATION '${locationPath}/${dateString}/'`;

        date.add(1, 'h');
    }

    return query.concat(';');
}