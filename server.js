const mongodb = require("mongodb");
const express = require('express');
const fs = require("fs");
const logger = require('morgan');
const errorhandler = require('errorhandler');
const bodyParser = require('body-parser');
const async = require('async');
const JSONStream = require('JSONStream');
const merge = require('deepmerge');
const extend = require('extend');

let app = express();
app.use(logger('dev'));
app.use(bodyParser.json());

const url = "mongodb://localhost:27017/edx-course-db";

//connect to database and parallel import
mongodb.MongoClient.connect(url, (error, db) => {
    if (error) return process.exit(1);

    deleteAllDocuments(db);

    var hrstart = process.hrtime();

    //Only allow set number of copy requests at a time
    var queue = async.queue((obj, callback) => {
        importDocument(obj, db, (item) => {
            //console.log(item);
            callback(item);
        });
    }, getItemsCount());

    //get data from JSON files, extend objects and push to queue
    fileToObjectArray('m3-customer-data.json', (data) => {
        fileToObjectArray('m3-customer-address-data.json', (addresses) => {
            for (var i = 0; i < data.length; i++) {
                queue.push(extend(data[i], addresses[i]));
            }
        });
    });

    //Check if we're done
    queue.drain = function () {
        if (queue.length() == 0) {   
            var hrend = process.hrtime(hrstart);         
            console.info("All items has been saved. Execution time (hr): %ds %dms", hrend[0], hrend[1]/1000000);
            getDocuments(db, (items) => {
                console.info("Number of DB items: " + items.length);
                 db.close();
            });
        }
    };
});

//delete all documents before inserts
const deleteAllDocuments = (db) => {
    const database = db.db("edx-course-db");
    var collection = database.collection('bitcoin-owners');
    collection.remove({});
}

//import documents to database
const importDocument = (data, db, callback) => {
    const database = db.db("edx-course-db");
    var collection = database.collection('bitcoin-owners');

    collection.insert(data, (error, result) => {
        if (error) return process.exit(1);

        callback(result);
    });
};

//get params form command line
const getItemsCount = () => {
    var paramsArray = process.argv.slice(2);

    if (paramsArray.length > 0)
        return paramsArray[0];
    else
        return 1;
}

//get imported documents from database
const getDocuments = (db, callback) => {
    const database = db.db("edx-course-db");
    database.collection('bitcoin-owners')
        .find({})
        .toArray((error, items) => {
            if (error) return process.exit(1);

            callback(items);
        });
};

//update document
// const updateDocument = (id, item, db, callback) => {
//     const database = db.db("edx-course-db");
//     database.collection('bitcoin-owners')
//         .update({ _id: id },
//         { $set: item },
//         (error, results) => {
//             if (error) return process.exit(1);
//             callback(results);
//         });
// };

//transform JSON file to array of objects
function fileToObjectArray(fileName, callback) {
    var transformStream = JSONStream.parse("*");
    var inputStream = fs.createReadStream(fileName);
    var objArray = [];

    inputStream
        .pipe(transformStream)
        .on("data",
        function handleRecord(data) {
            objArray.push(data);
        }).on("end",
        function handleEnd() {
            return callback(objArray);
        })
}

app.use(errorhandler());
app.listen(3000);