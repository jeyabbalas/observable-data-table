import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import terser from '@rollup/plugin-terser';
import replace from '@rollup/plugin-replace';

const isProduction = process.env.NODE_ENV === 'production';

export default [
  // Main library bundle
  {
    input: 'src/index.js',
    output: [
      {
        file: 'dist/datatable.js',
        format: 'es',
        sourcemap: true
      },
      {
        file: 'dist/datatable.min.js',
        format: 'es',
        plugins: [terser()],
        sourcemap: true
      }
    ],
    plugins: [
      replace({
        'process.env.NODE_ENV': JSON.stringify(isProduction ? 'production' : 'development'),
        preventAssignment: true
      }),
      resolve({
        browser: true,
        preferBuiltins: false
      }),
      commonjs()
    ],
    external: [
      '@duckdb/duckdb-wasm',
      '@uwdata/mosaic-core',
      '@uwdata/mosaic-sql', 
      '@uwdata/flechette',
      'apache-arrow',
      'd3',
      '@preact/signals-core',
      'codemirror',
      '@codemirror/lang-sql',
      '@codemirror/view',
      '@codemirror/state',
      '@codemirror/commands',
      '@codemirror/search',
      '@codemirror/autocomplete'
    ]
  },
  // Web Worker bundle
  {
    input: 'src/workers/duckdb.worker.js',
    output: {
      file: 'dist/workers/duckdb.worker.js',
      format: 'es',
      sourcemap: true
    },
    plugins: [
      replace({
        'process.env.NODE_ENV': JSON.stringify(isProduction ? 'production' : 'development'),
        preventAssignment: true
      }),
      resolve({
        browser: true,
        preferBuiltins: false
      }),
      commonjs(),
      ...(isProduction ? [terser()] : [])
    ]
  }
];