import { Readable } from "stream";
import { StreamWriter } from "n3";

function streamParseQuads(quads) {
    const reader = Readable.from(quads, { objectMode: true });
    const writer = new StreamWriter({ format: "N-Triples" });

    return reader.pipe(writer);
}

export const DROP = (targetGraph, queryBuilder) => {
    queryBuilder.push(`DROP SILENT ${targetGraph ? `GRAPH <${targetGraph}>` : "DEFAULT"};\n`);
};

export const INSERT = async (quads, targetGraph, queryBuilder) => {
    queryBuilder.push(`INSERT DATA {\n`);

    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await (const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await (const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
    }
    queryBuilder.push(`};\n`);
}

export const DELETE = async (quads, targetGraph, queryBuilder) => {
    // Get all unique subjects
    const subjectSet = new Set();
    quads.map(q => subjectSet.add(q.subject.value));

    // Build independent DELETE queries for each subject due to performance
    Array.from(subjectSet).forEach(subject => {
        if (targetGraph) {
            queryBuilder.push(`DELETE WHERE { GRAPH <${targetGraph}> { <${subject}> ?p ?o } };\n`);
        } else {
            queryBuilder.push(`DELETE WHERE { <${subject}> ?p ?o };\n`);
        }
    });
}