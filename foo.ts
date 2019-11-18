import * as moment from "moment-timezone";

export const foo = () => {
    return "foo"
}

export const date = () => {
    return moment("2017-01-01T00:15:48Z").utc().format("YYYY/MM/DD/HH");
}