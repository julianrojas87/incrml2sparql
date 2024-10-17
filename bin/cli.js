#!/usr/bin/env node

import { Command, Option } from "commander";
import { rdfDereferencer } from "rdf-dereference";
import { RdfStore } from "rdf-stores";
import { DataFactory } from "rdf-data-factory";
import { EVENTS } from "../lib/events.js";
import { DROP_INSERT, DELETE, UPDATE, INSERT } from "../lib/queries.js";

const program = new Command();
const df = new DataFactory();
let filePath = null;
let source = null;
let targetGraph = null;

program.arguments("<file>")
    .addOption(
        new Option("-s, --source <source>", "Source type")
            .choices(Object.keys(EVENTS))
    ).option(
        "-t, --target-graph <targetGraph>",
        "IRI of the target named graph to write towards"
    ).action((file, program) => {
        filePath = file;

        if (program.source) {
            source = program.source;
        } else {
            throw new Error("No source specified. Please use the -s option to specify the source type.");
        }

        if (program.targetGraph) {
            targetGraph = program.targetGraph;
        }
    });
program.parse(process.argv);


async function readRDFQuads(filePath, store) {
    const { data } = await rdfDereferencer.dereference(filePath, { localFiles: true });

    return new Promise((resolve, reject) => {
        data.on('data', (quad) => {
            store.addQuad(quad);
        });
        data.on('error', (err) => reject());
        data.on('end', () => resolve());
    });
}

function materializeMembers(quads, versionOfPath, store) {
    const materializedMembers = [];

    // Get all unique subjects
    const subjectSet = new Set();
    quads.map(q => subjectSet.add(q.subject));

    // Iterate over every member and adjust its properties
    for (const subject of Array.from(subjectSet)) {
        const canonicalSubject = store.getQuads(subject, versionOfPath, null, null)[0].object;
        const memberQuads = store.getQuads(subject, null, null, null);
        memberQuads.forEach(q => {
            materializedMembers.push(df.quad(canonicalSubject, q.predicate, q.object));
        });
    }

    return materializedMembers;
}

async function main() {
    if (filePath === null) {
        console.error("No source file specified");
        process.exit(1);
    }

    const store = RdfStore.createDefault();

    try {
        // Read the RDF quads from the file
        await readRDFQuads(filePath, store);

        // Check if we are dealing with a CHANGE case by looking for the corresponding named graphs
        const created = store.getQuads(null, null, null, df.namedNode(EVENTS[source].create));
        const updated = store.getQuads(null, null, null, df.namedNode(EVENTS[source].update));
        const deleted = store.getQuads(null, null, null, df.namedNode(EVENTS[source].delete));

        if (created.length > 0 || updated.length > 0 || deleted.length > 0) {
            // This is a CHANGE case!

            // Get the ldes:versionOfPath value
            const versionOfPath = store.getQuads(
                null,
                df.namedNode("https://w3id.org/ldes#versionOfPath"),
                null,
                null
            )[0].object;

            // Create the corresponding DELETE, UPDATE and INSERT queries
            const finalQuery = [];

            if (deleted.length > 0) {
                finalQuery.push(DELETE(materializeMembers(deleted, versionOfPath, store), targetGraph));
            }

            if (updated.length > 0) {
                finalQuery.push(UPDATE(materializeMembers(updated, versionOfPath, store), targetGraph));
            }

            if (created.length > 0) {
                finalQuery.push(INSERT(materializeMembers(created, versionOfPath, store), targetGraph));
            }

            console.log(finalQuery.join("\n"));
        } else {
            // This is an ALL case!
            const members = [];
            // Check if it contains LDES metadata
            const ldesMetadata = store.getQuads(null, df.namedNode("https://w3id.org/tree#member"), null, null);
            const hasLDES = store.getQuads(null, df.namedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), df.namedNode("https://w3id.org/ldes#EventStream"), null);

            if (ldesMetadata.length > 0) {
                // Extract the quads of the members only
                ldesMetadata.forEach(q => {
                    members.push(...store.getQuads(q.object, null, null, null));
                });
	
                // Create a corresponding DROP INSERT query
                console.log(DROP_INSERT(members, targetGraph));
            // ALL case if not an empty LDES
            } else if (hasLDES.length == 0) {
                // Extract all quads
                members.push(...store.getQuads(null, null, null, null));
                // Create a corresponding DROP INSERT query
                console.log(DROP_INSERT(members, targetGraph));
            }
        }

    } catch (err) {
        console.error(err);
        process.exit(1);
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
