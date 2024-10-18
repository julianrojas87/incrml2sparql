import { Readable, Transform } from "stream";
import { StreamWriter } from "n3";

function streamParseQuads(quads) {
    const reader = Readable.from(quads, { objectMode: true });
    const writer = new StreamWriter({ format: "N-Triples" });

    return reader.pipe(writer);
}

function streamQueryPattern(members) {
    const reader = Readable.from(members);
    let i = 0;
    const writer = new Transform({ transform: (chunk, _, done) => {
        const bgp = `<${chunk}> ?p_${i} ?o_${i}.\n`;
        i++
        done(null, bgp);
    }});

    return reader.pipe(writer);
}

export const DROP_INSERT = async (quads, targetGraph, queryBuilder) => {
    queryBuilder.push(`DROP SILENT ${targetGraph ? `<${targetGraph}>` : "DEFAULT"};\n`);
    queryBuilder.push(`INSERT DATA {\n`);

    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await(const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await(const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
    }
    queryBuilder.push(`}\n`);
};

export const INSERT = async (quads, targetGraph, queryBuilder) => {
    queryBuilder.push(`INSERT DATA {\n`);

    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await(const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await(const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
    }
    queryBuilder.push(`};\n`);
}

export const UPDATE = async (quads, targetGraph, queryBuilder) => {
    // Get all unique subjects
    const subjectSet = new Set();
    quads.map(q => subjectSet.add(q.subject.value));

    // DELETE
    queryBuilder.push(`DELETE { \n`);
    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        } 
    }   
    queryBuilder.push(`}\n`);

    // INSERT
    queryBuilder.push(`INSERT { \n`);
    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await(const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await(const chunk of streamParseQuads(quads)) {
            queryBuilder.push(chunk);
        }
    }
    queryBuilder.push(`}\n`);

    // WHERE
    queryBuilder.push(`WHERE { \n`);
    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        }
    }
    queryBuilder.push(`};\n`);
}

export const DELETE = async (quads, targetGraph, queryBuilder) => {
    // Get all unique subjects
    const subjectSet = new Set();
    quads.map(q => subjectSet.add(q.subject.value));
    
    // DELETE
    queryBuilder.push(`DELETE { \n`);
    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        } 
    }   
    queryBuilder.push(`}\n`);

    // WHERE
    queryBuilder.push(`WHERE { \n`);
    if (targetGraph) {
        queryBuilder.push(`GRAPH <${targetGraph}> {\n`);
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        }
        queryBuilder.push(`}\n`);
    } else {
        for await(const chunk of streamQueryPattern(Array.from(subjectSet))) {
            queryBuilder.push(chunk);
        }
    }
    queryBuilder.push(`};\n`);
}