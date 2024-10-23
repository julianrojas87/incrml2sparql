#!/usr/bin/env node

import { Command, Option } from "commander";
import { Readable } from "stream";
import { rdfDereferencer } from "rdf-dereference";
import { RdfStore } from "rdf-stores";
import { DataFactory } from "rdf-data-factory";
import { EVENTS } from "../lib/events.js";
import { DROP, DELETE, INSERT } from "../lib/queries.js";

const program = new Command();
const df = new DataFactory();
let filePath = null;
let source = null;
let targetGraph = null;
let limit = 0;

program.arguments("<file>")
    .addOption(
        new Option("-s, --source <source>", "Source type")
            .choices(Object.keys(EVENTS))
    ).option(
        "-t, --target-graph <targetGraph>",
        "IRI of the target named graph to write towards"
    ).option(
        "-l, --limit <limit>",
        "Maximum number of triples to insert in a single query. Larger queries will be split accordingly."
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

        if (program.limit) {
            limit = parseInt(program.limit);
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

    // Global quad store
    const store = RdfStore.createDefault();
    // Stream query builder
    const queryBuilder = new Readable({ read() { } });
    // Pipe to standard output
    queryBuilder.pipe(process.stdout);

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
            if (deleted.length > 0) {
                const materializedMembers = materializeMembers(deleted, versionOfPath, store);
                await DELETE(materializedMembers, targetGraph, queryBuilder);
            }

            if (updated.length > 0) {
                // We do updates as 2 separate queries DELETE and INSERT DATA, due to performance
                const materializedMembers = materializeMembers(updated, versionOfPath, store);
                await DELETE(materializedMembers, targetGraph, queryBuilder);
                if (limit > 0 && updated.length > limit) {
                    for (let i = 0; i < materializedMembers.length; i += limit) {
                        await INSERT(materializedMembers.slice(i, i + limit), targetGraph, queryBuilder);
                    }
                } else {
                    // Create a corresponding INSERT query
                    await INSERT(materializedMembers, targetGraph, queryBuilder);
                }
            }

            if (created.length > 0) {
                // Split the created members in multiple INSERT queries if number of quads exceeds the limit 
                const materializedMembers = materializeMembers(created, versionOfPath, store);
                if (limit > 0 && created.length > limit) {
                    for (let i = 0; i < materializedMembers.length; i += limit) {
                        await INSERT(materializedMembers.slice(i, i + limit), targetGraph, queryBuilder);
                    }
                } else {
                    // Create a corresponding INSERT query
                    await INSERT(materializedMembers, targetGraph, queryBuilder);
                }
            }
        } else {
            // This is an ALL case!
            const memberQuads = [];
            // Check if it contains LDES metadata
            const ldesMetadata = store.getQuads(null, df.namedNode("https://w3id.org/tree#member"), null, null);
            const hasLDES = store.getQuads(
                null,
                df.namedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                df.namedNode("https://w3id.org/ldes#EventStream"),
                null
            );

            if (ldesMetadata.length > 0) {
                // Extract the quads of the members only
                ldesMetadata.forEach(q => {
                    memberQuads.push(...store.getQuads(q.object, null, null, null));
                });
                // ALL case if not an empty LDES
            } else if (hasLDES.length === 0) {
                // Extract all quads
                memberQuads.push(...store.getQuads(null, null, null, null));
            }

            // Do a DROP query to delete the older KG
            if (memberQuads.length > 0) {
                DROP(targetGraph, queryBuilder);

                if (limit > 0 && memberQuads.length > limit) {
                    // Create multiple INSERT queries with a limited amount of triples
                    for (let i = 0; i < memberQuads.length; i += limit) {
                        await INSERT(memberQuads.slice(i, i + limit), targetGraph, queryBuilder);
                    }
                } else {
                    // Create a corresponding DROP INSERT query
                    await INSERT(memberQuads, targetGraph, queryBuilder);
                }
	    }
        }
        // Close the query stream
        queryBuilder.push(null);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
