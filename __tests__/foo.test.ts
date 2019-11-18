import { foo, date } from "../foo";
import * as moment from "moment-timezone";

test("foo", ()=>{
    expect(foo()).toEqual("foo");
});

test("date", ()=>{
    expect(date()).toEqual('2017/01/01/00');
});

// test("dateYear", ()=>{
//     const dateTest = date();
//     expect(`${dateTest.getUTCFullYear()}`).toEqual('2017');
// });

// test("dateMonth", ()=>{
//     expect(date().getUTCMonth() + 1).toEqual(1);
// });

// test("dateDay", ()=>{
//     expect(date().getUTCDate()).toEqual(1);
// });

// test("dateHour", ()=>{
//     expect(date().getUTCHours()).toEqual(0);
// });