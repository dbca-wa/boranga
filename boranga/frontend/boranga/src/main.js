import 'vite/modulepreload-polyfill';

import { createApp } from 'vue';
import App from './App.vue';
import router from './router';
import helpers from '@/utils/helpers';

import $ from 'jquery';
import select2 from 'select2';
window.$ = $;
import swal from 'sweetalert2';
window.swal = swal;
select2();

import 'datatables.net-bs5';
import 'datatables.net-buttons-bs5';
import 'datatables.net-responsive-bs5';
import 'datatables.net-buttons/js/dataTables.buttons.js';
// jszip used for exporting to .xlsx from datatables
import JSZip from 'jszip';
window.JSZip = JSZip;
import 'datatables.net-buttons/js/buttons.html5.js';
import 'select2';
import 'jquery-validation';

import 'sweetalert2/dist/sweetalert2.css';
import 'select2/dist/css/select2.min.css';
import 'select2-bootstrap-5-theme/dist/select2-bootstrap-5-theme.min.css';
import '@/../node_modules/datatables.net-bs5/css/dataTables.bootstrap5.min.css';
import '@/../node_modules/datatables.net-responsive-bs5/css/responsive.bootstrap5.min.css';
import 'bootstrap-icons/font/bootstrap-icons.css';

const app = createApp(App);

const originalFetch = window.fetch.bind(window);

// Do NOT make the outer wrapper async; otherwise window.fetch becomes a Promise.
window.fetch = ((orig) => {
    return async (...args) => {
        // Normalize URL (string | URL | Request)
        let url;
        if (args[0] instanceof Request) {
            url = new URL(args[0].url, window.location.origin);
        } else if (args[0] instanceof URL) {
            url = new URL(args[0].href, window.location.origin);
        } else {
            url = new URL(String(args[0]), window.location.origin);
        }
        const sameOrigin = url.origin === window.location.origin;
        const isApi = sameOrigin && url.pathname.startsWith('/api');

        // Merge headers
        let headers = new Headers();
        if (args.length > 1 && args[1]?.headers) {
            headers =
                args[1].headers instanceof Headers
                    ? new Headers(args[1].headers)
                    : new Headers(args[1].headers);
        } else if (args[0] instanceof Request && args[0].headers) {
            headers = new Headers(args[0].headers);
        }

        if (sameOrigin)
            headers.set('X-CSRFToken', helpers.getCookie('csrftoken'));
        if (
            sameOrigin &&
            args.length > 1 &&
            typeof args[1]?.body === 'string'
        ) {
            headers.set('Content-Type', 'application/json');
        }

        if (args.length > 1) {
            args[1].headers = headers;
        } else {
            args.push({ headers });
        }

        const response = await orig(...args);

        if (
            (response.status === 401 && isApi) ||
            (response.status === 403 && isApi)
        ) {
            window.location.href =
                '/login/?next=' +
                encodeURIComponent(
                    window.location.pathname +
                        window.location.search +
                        window.location.hash
                );
            return response;
        } else if (response.status === 403) {
            swal.fire({
                icon: 'error',
                title: 'Access Denied',
                text: 'You do not have permission to perform this action.',
                customClass: { confirmButton: 'btn btn-primary' },
            });
        }
        return response;
    };
})(originalFetch);

app.use(router);
router.isReady().then(() => app.mount('#app'));
