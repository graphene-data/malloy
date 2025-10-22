/* eslint-disable node/no-extraneous-import */
import {SingleConnectionRuntime, InMemoryURLReader} from '@malloydata/malloy';
import {DuckDBConnection} from '@malloydata/db-duckdb';
// import path from 'path';

async function main() {
  const duckdbPath = ':memory:';
  const connection = new DuckDBConnection('duckdb', duckdbPath);
  const runtime = new SingleConnectionRuntime({
    urlReader: new InMemoryURLReader(new Map()),
    connection,
  });

  const initialModel = `
    source: alpha is duckdb.sql("""
      select 1 as id, 'alpha' as a_name
    """)

    source: beta is duckdb.sql("""
      select 1 as id, 'beta' as b_name, 1 as alpha_id
    """) extend {
      join_one: alpha is alpha on alpha.id = alpha_id
    }

    # query-based source that aggregates over alpha
    query: gamma_q is alpha -> {
      group_by: id
      aggregate: total_rows is count()
    }
    source: gamma is gamma_q extend {
      # pseudo-primary key
      primary_key: id
    }
  `;

  const model = runtime.loadModel(initialModel);
  const compiled = await model.getModel();
  const md = compiled._modelDef as any

  // append join from alpha -> beta
  md.contents['alpha'].fields.push({
    ...md.contents['beta'],
    name: 'beta',
    join: 'many',
    onExpression: {
      node: '=',
      kids: {
        left: {node: 'field', path: ['id']},
        right: {node: 'field', path: ['beta', 'alpha_id']},
      },
    },
  });

  // Also join aggregated gamma back into alpha and beta to create a richer cycle
  md.contents['alpha'].fields.push({
    ...md.contents['gamma'],
    name: 'gamma',
    join: 'one',
    onExpression: {
      node: '=',
      kids: {
        left: {node: 'field', path: ['id']},
        right: {node: 'field', path: ['gamma', 'id']},
      },
    },
  });

  const traverseJoinQuery = `
    run: beta -> {
      group_by: b_name
      group_by: alpha.a_name
    }
  `;

  const res = await model.loadQuery(traverseJoinQuery).run();
  console.log('Traverse join result rows:', res.data.value);

  // More robust cyclical traversals hitting gamma aggregations
  const moreQueries = [
    'run: alpha -> { group_by: a_name; group_by: beta.b_name; group_by: gamma.total_rows }',
    'run: beta -> { group_by: b_name; group_by: alpha.a_name; group_by: alpha.gamma.total_rows }',
    // deliberately traverse across the cycle twice
    'run: alpha -> { group_by: gamma.total_rows }',
  ];
  for (const q of moreQueries) {
    try {
      const r = await model.loadQuery(q).run();
      console.log('Query OK:', q.replace(/\s+/g, ' '), '=>', r.data.value);
    } catch (e) {
      console.log('Query ERR:', q.replace(/\s+/g, ' '), String(e));
    }
  }
}

main();
