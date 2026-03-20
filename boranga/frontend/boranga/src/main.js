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

// --- Read-only mode helpers ---
const READ_ONLY_TOAST_ID = 'boranga-read-only-toast';

function showReadOnlyBanner() {
    if (document.getElementById(READ_ONLY_TOAST_ID)) return;

    // Toast container — fixed top-right, no page height impact
    const container = document.createElement('div');
    container.style.cssText = 'position:fixed;top:6px;right:1rem;z-index:9999;';

    container.innerHTML = `
        <div id="${READ_ONLY_TOAST_ID}" class="toast align-items-center border-0 show" role="alert" aria-live="assertive" aria-atomic="true" style="background-color:#92400e;color:#fff;">
            <div class="d-flex">
                <div class="toast-body fw-semibold">
                    \u{1F512} System READ-ONLY for data verification
                </div>
            </div>
        </div>`;

    document.body.appendChild(container);
}

function hideReadOnlyBanner() {
    document
        .getElementById(READ_ONLY_TOAST_ID)
        ?.closest('div[style]')
        ?.remove();
}

/** Fetches the current read-only status from the server and updates window.env + banner. */
function refreshReadOnlyStatus() {
    return originalFetch('/api/read_only_status')
        .then((r) => r.json())
        .then((data) => {
            window.env = window.env || {};
            window.env.read_only = !!data.read_only;
            if (window.env.read_only) {
                showReadOnlyBanner();
            } else {
                hideReadOnlyBanner();
            }
            return window.env.read_only;
        })
        .catch(() => false);
}

// Check on initial page load.
refreshReadOnlyStatus();

// Do NOT make the outer wrapper async; otherwise window.fetch becomes a Promise.
window.fetch = ((orig) => {
    const WRITE_METHODS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

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

        // Determine request method
        const method = (
            (args.length > 1 && args[1]?.method) ||
            (args[0] instanceof Request && args[0].method) ||
            'GET'
        ).toUpperCase();

        // Re-check read-only status from the server on every write attempt.
        // DataTables uses POST on _paginated endpoints when the querystring is too
        // long — these are read-only list queries and must not be blocked.
        if (
            isApi &&
            WRITE_METHODS.has(method) &&
            !url.pathname.includes('_paginated')
        ) {
            const isReadOnly = await refreshReadOnlyStatus();
            if (isReadOnly) {
                swal.fire({
                    iconHtml:
                        '<i class="bi bi-lock-fill" style="font-size:3rem;color:#92400e;"></i>',
                    customClass: {
                        icon: 'border-0',
                        confirmButton: 'btn btn-primary',
                    },
                    title: 'Read-Only Mode',
                    text: 'The system is currently in read-only mode for data verification. No changes can be made at this time.',
                });
                return new Response(
                    JSON.stringify({
                        detail: 'The system is currently in read-only mode for data verification.',
                    }),
                    {
                        status: 503,
                        headers: { 'Content-Type': 'application/json' },
                    }
                );
            }
        }

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
                '/?next=' +
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
