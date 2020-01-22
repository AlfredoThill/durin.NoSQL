// Sample JSON_
// {
//     "_id" : ObjectId("5d978ef7048b6e21540ea0ff"),
//     "A" : 2017,
//     "PROG" : "CEBAS",
//     "REG" : 1,
//     "DIST" : "LA PLATA",
//     "TEST" : "CEBAS",
//     "EST" : 1,
//     "CUEEST" : 60900600,
//     "AP" : "SURNAME",
//     "NOM" : "NAME",
//     "CUIL" : 27361106173,
//     "DNI" : 36110617,
//     "FNAC" : 31449,
//     "GEN" : "MASCULINO",
//     "ACUR" : 1,
//     "ISO_DATE" : ISODate("1986-02-06T00:00:00Z")
// }
// -------------------------------------------------------------------

//Code
const path = require('path');
const xlsx = require('xlsx');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const uri = "mongodb://localhost";

//cuantos estudiante de "ofertaX" se encuentran tmb en otra oferta/s, por ciclo electivo
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
client.connect( async function () {
    try {
     const collection = await client.db("test").collection('estudiantes');
     const stages = [
        {$match: { A: Number(2018), DNI: {$exists: true}}}, //filtro por 2018, DNI declarado y numerico
        {$group : { _id: "$DNI" , programas: { $addToSet: "$PROG" }, tounwind: { $addToSet: "$PROG" } ,count : { $sum: 1} } }, //agrupo(saco unicos) por dni y agrego: programas y cuenta
        {$match: { "count": { $gt:1 } } }, //filtro por cuenta mayor a 1, dejo solo los duplicados
        {$addFields: { size: {$size: "$tounwind"} } }, //desarmo uno de los array "Programas" para ver en cuantos programas aparece
        {$match: { "size": { $gt:1 } } }, //filtro por los q aparecen en mas de una oferta
        {$unwind : "$tounwind" }, //desarmo cada entrada en una por oferta en la que aparece
        {$match: { tounwind: "FINES DEUDORES"} }, //filtro por la oferta objetivo
        {$group: { _id: null , Duplicados: { $sum: 1} }}, //agrupo para contar duplicados
        {$project: { _id: 1, Duplicados: 1 }} //projección
     ];
     const cursor = await collection.aggregate(stages,{allowDiskUse: true});
    //  await cursor.forEach( function(doc) { console.log(doc)});
     const results = [];
     await cursor.forEach( function(doc) {
     const data = [];
        for (key in doc) { if (key != "_id") {data.push(doc[key])}}
     results.push(data);  
     });
     results.sort();
     const workbook = xlsx.utils.book_new();
     const newsheet = xlsx.utils.aoa_to_sheet(results);
     const appendsheet = xlsx.utils.book_append_sheet(workbook,newsheet,"Results");
     xlsx.writeFile(workbook,"Resultados.xlsx");
     console.log("Done");
    client.close();    
    }
    catch(e) {
        console.log('Este es el error: '+e);
        client.close();
    } 
});       