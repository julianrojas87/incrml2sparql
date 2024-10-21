#!/usr/bin/env node

import { Command, Option } from "commander";
import * as fs from "fs";

const program = new Command();
let directoryPath = null;
let fileNames = [];
let metadata = {
    "@id": "",
    "name": "",
    "description": "SPARQL queries from IncRML2SPARQL on Virtuoso",
    "steps": [
        {
            "@id": "http://example.com/test-cases/incrml2sparql#step1",
            "name": "Virtuoso SPARQL UPDATE endpoint",
            "resource": "Virtuoso",
            "command": "wait_until_ready",
            "parameters": {}
        },
    ]
}

let queryStep = {
    "@id": "",
    "name": "",
    "resource": "Query",
    "command": "execute_from_file",
    "parameters": {
        "query_file": "",
        "sparql_endpoint": "http://dba:root@localhost:8890/sparql-auth",
        "auth": {
            "type": "digest",
            "username": "dba",
            "password": "root"
	}
    }
}

program.arguments("<directory>")
    .option(
        "<directory>",
	"Directories where all queries are listed"
    ).action((directory, program) => {
        directoryPath = directory;
    });
program.parse(process.argv);


async function main() {
    if (directoryPath === null) {
        console.error("No directory specified");
        process.exit(1);
    }

    /* Make directory structure for KROWN */
    fs.mkdirSync("cases/" + directoryPath, { recursive : true });
    fs.mkdirSync("cases/" + directoryPath + "data/shared", { recursive : true });

    /* Copy queries */
    fs.readdirSync(directoryPath).forEach(file => {
        const fileSize = fs.statSync(directoryPath + file).size;
        if (file.endsWith(".sparql") && fileSize > 0) {
            console.log(directoryPath + file);
            fs.copyFileSync(directoryPath + file, "cases/" + directoryPath + "data/shared/" + file);
            fileNames.push(file);
        }
    })

    /* Make metadata.json pipeline for KROWN */
    metadata['@id'] = 'http://example.org/incrml2sparql/' + directoryPath;
    metadata['name'] = directoryPath.replaceAll('/', ' ');
    let stepNumber = 2;
    fileNames.forEach((file) => {
	let step = JSON.parse(JSON.stringify(queryStep));
        step['@id'] = metadata['@id'] + '#step' + stepNumber;
        step['name'] = 'SPARQL query ' + file;
        step['parameters']['query_file'] = file;
        metadata['steps'].push(step);
        stepNumber++;
    });
    fs.writeFileSync("cases/" + directoryPath + "metadata.json", JSON.stringify(metadata, null, 4));
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
