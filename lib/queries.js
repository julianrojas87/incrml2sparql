import { Writer as N3Writer } from "n3"

export const DROP_INSERT = (quads, targetGraph) => {
    const insertData = new N3Writer().quadsToString(quads);
    return `
        DROP SILENT ${targetGraph ? `<${targetGraph}>` : "DEFAULT"};
        INSERT DATA {
            ${targetGraph ? `GRAPH <${targetGraph}> {${insertData}}` : insertData}
        }
    `;
};

export const INSERT = (quads, targetGraph) => {
    const insertData = new N3Writer().quadsToString(quads);
    return `
        INSERT DATA {
            ${targetGraph ? `GRAPH <${targetGraph}> {${insertData}}` : insertData}
        };
    `;
}

export const UPDATE = (quads, targetGraph) => {
    // Get all unique subjects
    const subjectSet = new Set();
    quads.map(q => subjectSet.add(q.subject.value));
    // Create BGPs
    const queryPattern = Array.from(subjectSet)
        .map((sub, i) => {
            return `<${sub}> ?p_${i} ?o_${i}.`
        }).join("\n");
    
    const insertData = new N3Writer().quadsToString(quads); 

    return `
        ${targetGraph ? `WITH <${targetGraph}>` : ""}
        DELETE { 
            ${targetGraph ? `GRAPH <${targetGraph}> {${queryPattern}}`: queryPattern} 
        }
        INSERT { 
            ${targetGraph ? `GRAPH <${targetGraph}> {${insertData}}` : insertData}
        }
        WHERE { 
            ${targetGraph ? `GRAPH <${targetGraph}> {${queryPattern}}`: queryPattern} 
        };
    `;
}

export const DELETE = (quads, targetGraph) => {
    // Get all unique subjects
    const subjectSet = new Set();
    quads.map(q => subjectSet.add(q.subject.value));
    // Create BGPs
    const queryPattern = Array.from(subjectSet)
        .map((sub, i) => {
            return `<${sub}> ?p_${i} ?o_${i}.`
        }).join("\n");

    return `
        DELETE { 
            ${targetGraph ? `GRAPH <${targetGraph}> {${queryPattern}}`: queryPattern} 
        }
        WHERE { 
            ${targetGraph ? `GRAPH <${targetGraph}> {${queryPattern}}`: queryPattern} 
        };
    `;
}