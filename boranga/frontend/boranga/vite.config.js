import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import path from 'path';
import eslint from 'vite-plugin-eslint';
import { viteStaticCopy } from 'vite-plugin-static-copy';
import svgLoader from 'vite-svg-loader';

const port = process.env.PORT ? parseInt(process.env.PORT) : 5173;
const host = process.env.HOST || '0.0.0.0';

export default defineConfig({
    base: '/static/boranga_vue/',
    server: {
        host: host,
        port: port,
        strictPort: true,
        open: false,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers':
                'Origin, X-Requested-With, Content-Type, Accept',
        },
        hmr: {
            protocol: 'ws',
            host: host,
            port: port,
        },
    },
    plugins: [
        vue(),
        eslint(),
        svgLoader(),
        viteStaticCopy({
            // Had to do this to get the relative paths to work
            // Probably a better way but I couldn't figure it out
            targets: [
                { src: 'src/assets', dest: 'src' },
                {
                    src: 'node_modules/@fortawesome/fontawesome-free/webfonts',
                    dest: 'node_modules/@fortawesome/fontawesome-free/',
                },
            ],
        }),
    ],
    resolve: {
        alias: {
            '@': path.resolve(__dirname, './src'),
            '@vue-utils': path.resolve(__dirname, 'src/utils/vue'),
            '@common-utils': path.resolve(__dirname, 'src/components/common/'),
            '@static-root': path.resolve(__dirname, '../../../staticfiles/'),
        },
    },
    build: {
        manifest: 'manifest.json',
        filenameHashing: false,
        commonjsOptions: { transformMixedEsModules: true },
        root: path.resolve(__dirname, './src'),
        outDir: path.resolve(__dirname, '../../static/boranga_vue'),
        publicPath: '/static/boranga_vue/',
        sourcemap: true,
        rollupOptions: {
            output: {
                entryFileNames: 'js/[name].js',
                chunkFileNames: 'js/[name].js',
                assetFileNames: '[ext]/[name].[ext]',
            },
        },
        emptyOutDir: true,
    },
});