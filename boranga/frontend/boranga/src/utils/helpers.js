import constants from './constants';

export default {
    truncate(text, { length = 30, omission = '...', separator } = {}) {
        if (text == null) return '';
        const str = String(text);
        if (str.length <= length) return str;
        const end = length - omission.length;
        if (end < 1) return omission;
        let slice = str.slice(0, end);
        if (separator) {
            const sepIdx = slice.lastIndexOf(separator);
            if (sepIdx > -1) {
                slice = slice.slice(0, sepIdx);
            }
        }
        return slice + omission;
    },
    escapeAttr(value) {
        // Normalize first so objects/arrays become readable JSON not [object Object]
        let str;
        if (value == null) {
            str = '';
        } else if (typeof value === 'string') {
            str = value;
        } else if (typeof value === 'number' || typeof value === 'boolean') {
            str = String(value);
        } else {
            try {
                str = JSON.stringify(value);
            } catch {
                str = String(value);
            }
        }
        return str
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    },
    escapeHtml: function (s) {
        if (!s) return '';
        return s.replace(
            /[&<>"']/g,
            (ch) =>
                ({
                    '&': '&amp;',
                    '<': '&lt;',
                    '>': '&gt;',
                    '"': '&quot;',
                    "'": '&#39;',
                })[ch]
        );
    },
    statusFromError: function (err) {
        const s = String((err && (err.detail || err.message)) || err || '');
        // Prefer "HTTP 502 Bad Gateway", else "HTTP 502", else bare "502"
        const m =
            s.match(/HTTP\s+\d{3}\s+[^\n<]+/i) ||
            s.match(/HTTP\s+\d{3}/i) ||
            s.match(/\b\d{3}\b/);
        return m ? m[0] : null;
    },
    formatError: function (err) {
        let returnStr = '';
        // object {}
        if (
            typeof err.body === 'object' &&
            !Object.prototype.hasOwnProperty.call(err.body, 'length')
        ) {
            for (const key of Object.keys(err.body)) {
                returnStr += `${key}: ${err.body[key]} <br/>`;
            }
            // array
        } else if (typeof err.body === 'object') {
            returnStr = err.body[0];
            // string
        } else {
            returnStr = err.body;
        }
        return returnStr;
    },
    apiError: function (resp) {
        var error_str = '';
        if (resp.status === 400) {
            try {
                let obj = JSON.parse(resp.responseText);
                error_str = obj.non_field_errors[0].replace(/[[\]"]/g, '');
            } catch (e) {
                console.log(e);
                error_str = resp.responseText.replace(/[[\]"]/g, '');
            }
        } else if (resp.status === 404) {
            error_str = 'The resource you are looking for does not exist.';
        } else {
            error_str = resp.responseText.replace(/[[\]"]/g, '');
        }
        return error_str;
    },
    apiVueResourceError: function (resp) {
        console.log('in apiVueResourceError');
        console.log(resp);
        var error_str = '';
        var text = null;
        if (resp.status === 400) {
            if (Array.isArray(resp.body)) {
                text = resp.body[0];
            } else if (typeof resp.body == 'object') {
                text = resp.body;
            } else {
                text = resp.body;
            }

            if (typeof text == 'object') {
                if (
                    Object.prototype.hasOwnProperty.call(
                        text,
                        'non_field_errors'
                    )
                ) {
                    error_str = text.non_field_errors[0].replace(/[[\]"]/g, '');
                } else {
                    console.log('text');
                    console.log(text);
                    for (let key in text) {
                        error_str += key + ': ' + text[key] + '<br/>';
                    }
                }
            } else {
                error_str = text.replace(/[[\]"]/g, '');
                error_str = text.replace(/^['"](.*)['"]$/, '$1');
            }
        } else if (resp.status === 404) {
            error_str = 'The resource you are looking for does not exist.';
        }
        console.log('error_str');
        console.log(error_str);
        return error_str;
    },
    renderErrorsAsHtml: function (errors) {
        const esc = (v) => {
            if (v === null || v === undefined) return '';
            return String(v)
                .replaceAll('&', '&amp;')
                .replaceAll('<', '&lt;')
                .replaceAll('>', '&gt;')
                .replaceAll('"', '&quot;')
                .replaceAll("'", '&#39;');
        };

        if (typeof errors === 'string') {
            return esc(errors);
        }

        if (Array.isArray(errors)) {
            if (errors.length === 0) return '';

            if (errors.length > 1) {
                // Prefer error.detail when present, otherwise show the element
                const items = errors
                    .map((err) => {
                        if (err && typeof err === 'object' && 'detail' in err) {
                            return `<li>${esc(err.detail)}</li>`;
                        }
                        return `<li>${esc(err)}</li>`;
                    })
                    .join('');
                return `<ul class="errors-list">${items}</ul>`;
            }

            // Single-element array
            const single = errors[0];
            if (single && typeof single === 'object') {
                const keys = Object.keys(single);
                if (keys.length > 1) {
                    if (
                        Object.prototype.hasOwnProperty.call(single, 'detail')
                    ) {
                        return esc(single.detail);
                    }
                    // Render object as list of key: value
                    const items = keys
                        .map(
                            (k) =>
                                `<li><span class="fw-bold">${esc(k)}</span>: ${esc(single[k])}</li>`
                        )
                        .join('');
                    return `<ul class="errors-list">${items}</ul>`;
                }
                // single-key object -> bold key: value
                const k = keys[0];
                return `<span class="fw-bold">${esc(k)}</span>: ${esc(single[k])}`;
            }

            // single primitive
            return esc(single);
        }

        if (errors && typeof errors === 'object') {
            const keys = Object.keys(errors);
            if (keys.length === 0) return '';
            if (keys.length > 1) {
                const items = keys
                    .map(
                        (k) =>
                            `<li><span class="fw-bold">${esc(k)}</span>: ${esc(errors[k])}</li>`
                    )
                    .join('');
                return `<ul class="errors-list">${items}</ul>`;
            }
            const k = keys[0];
            return `<span class="fw-bold">${esc(k)}</span>: ${esc(errors[k])}`;
        }

        // Fallback: stringify other types
        return esc(errors);
    },
    goBack: function (vm) {
        vm.$router.go(window.history.back());
    },
    copyObject: function (obj) {
        return JSON.parse(JSON.stringify(obj));
    },
    getCookie: function (name) {
        var value = null;
        if (document.cookie && document.cookie !== '') {
            var cookies = document.cookie.split(';');
            for (var i = 0; i < cookies.length; i++) {
                var cookie = cookies[i].trim();
                if (
                    cookie.substring(0, name.length + 1).trim() ===
                    name + '='
                ) {
                    value = decodeURIComponent(
                        cookie.substring(name.length + 1)
                    );
                    break;
                }
            }
        }
        return value;
    },
    namePopover: function ($, vmDataTable) {
        vmDataTable.on('mouseover', '.name_popover', function () {
            $(this).popover('show');
            $(this).on('mouseout', function () {
                $(this).popover('hide');
            });
        });
    },
    add_endpoint_json: function (string, addition) {
        let res = string.split('.json');
        let endpoint = res[0] + '/' + addition + '.json';
        endpoint = endpoint.replace('//', '/'); // Remove duplicated '/' just in case
        return endpoint;
    },
    add_endpoint_join: function (api_string, addition) {
        // assumes api_string has trailing forward slash "/" character required for POST
        let endpoint = api_string + addition;
        endpoint = endpoint.replace('//', '/'); // Remove duplicated '/' just in case
        return endpoint;
    },
    dtPopover(value, truncate_length = 30, trigger = 'hover') {
        const ellipsis = '...';
        const raw = value == null ? '' : String(value);
        const truncated = this.truncate(raw, {
            length: truncate_length,
            omission: ellipsis,
            separator: ' ',
        });
        let result = '<span>' + truncated + '</span>';
        if (raw.length > truncated.length) {
            result += `<a class="mx-0 ps-1 pe-0" href="javascript://"
                role="button"
                data-bs-toggle="popover"
                data-bs-trigger="${this.escapeAttr(trigger)}"
                data-bs-placement="top"
                data-bs-html="true"
                data-bs-content="${this.escapeAttr(raw)}"
            ><small>hover for more</small></a>`;
        }
        return result;
    },
    dtPopoverSplit(value, truncate_length = 30, trigger = 'hover') {
        const ellipsis = '...';
        const raw = value == null ? '' : String(value);
        const truncated = this.truncate(raw, {
            length: truncate_length,
            omission: ellipsis,
            separator: ' ',
        });
        const text = '<span>' + truncated + '</span>';
        if (raw.length > truncated.length) {
            const link = `<a class="mx-0 ps-1 pe-0" href="javascript://"
                role="button"
                data-bs-toggle="popover"
                data-bs-trigger="${this.escapeAttr(trigger)}"
                data-bs-placement="top"
                data-bs-html="true"
                data-bs-content="${this.escapeAttr(raw)}"
            ><small>hover for more</small></a>`;
            return { text, link };
        }
        return { text, link: '' };
    },
    processError: async function (err) {
        console.log(err);
        let errorText = '';
        if (err.body.non_field_errors) {
            console.log('non_field_errors');
            // When non field errors raised
            for (let i = 0; i < err.body.non_field_errors.length; i++) {
                errorText += err.body.non_field_errors[i] + '<br />';
            }
        } else if (Array.isArray(err.body)) {
            console.log('isArray');
            // When serializers.ValidationError raised
            for (let i = 0; i < err.body.length; i++) {
                errorText += err.body[i] + '<br />';
            }
        } else {
            console.log('else');
            // When field errors raised
            for (let field_name in err.body) {
                if (
                    Object.prototype.hasOwnProperty.call(err.body, field_name)
                ) {
                    errorText += field_name + ':<br />';
                    for (let j = 0; j < err.body[field_name].length; j++) {
                        errorText += err.body[field_name][j] + '<br />';
                    }
                }
            }
        }
        await swal('Error', errorText, 'error');
    },
    post_and_redirect: function (url, postData) {
        /* http.post and ajax do not allow redirect from Django View (post method),
           this function allows redirect by mimicking a form submit.

           usage:  vm.post_and_redirect(vm.application_fee_url, {'csrfmiddlewaretoken' : vm.csrf_token});
        */
        var postFormStr = "<form method='POST' action='" + url + "'>";

        for (var key in postData) {
            if (Object.prototype.hasOwnProperty.call(postData, key)) {
                postFormStr +=
                    "<input type='hidden' name='" +
                    key +
                    "' value='" +
                    postData[key] +
                    "'>";
            }
        }
        postFormStr += '</form>';
        var formElement = $(postFormStr);
        $('body').append(formElement);
        $(formElement).submit();
    },
    enablePopovers: function () {
        let popoverTriggerList = [].slice.call(
            document.querySelectorAll('[data-bs-toggle="popover"]')
        );
        popoverTriggerList.map(function (popoverTriggerEl) {
            new bootstrap.Popover(popoverTriggerEl);
        });
    },
    checkForChange: function (before, after) {
        //compare two objects, return true if the are the same
        console.log(before);
        console.log(after);
        //JSON.stringify does not guarantee order and removes keys with undefined values
        //that is acceptable for this use case, however
        return JSON.stringify(before) === JSON.stringify(after);
    },
    getNested(obj, path) {
        return path.split('.').reduce((o, p) => (o ? o[p] : undefined), obj);
    },
    setNested(obj, path, value) {
        const parts = path.split('.');
        const last = parts.pop();
        const target = parts.reduce((o, p) => (o ? o[p] : undefined), obj);
        if (target && last) target[last] = value;
    },
    /**
     * Confirm the change of species or community name in selecting/unselecting select2 event.
     * @param {Object} event - The event object.
     * @param {HTMLElement} selector - The selector for the element to change.
     * @param {string=} type - The type of change (species or community).
     * @param {boolean=} hasRelatedReports - Whether the occurrence has related occurrence reports.
     */
    confirmChangeOfSpeciesOrCommunityName: function (
        event,
        selector,
        type = '',
        hasRelatedReports = true
    ) {
        // Prevent firing select/unselect events, we do that ourselves below
        event.preventDefault();

        let newData = null;
        if (event.params.args._type === 'select') {
            newData = event.params.args.data;
        } else if (event.params.args._type === 'unselect') {
            // Unselecting means no new value
        }
        console.log(`Changing to ${newData?.id}`);

        const applyChange = () => {
            $(selector).trigger({
                type: `select2:${newData === null ? 'un' : ''}select`,
                params: {
                    data: newData,
                },
            });

            const optionExists =
                $(`#${selector.id} option`).filter(
                    (_, option) => option.value === String(newData?.id)
                ).length > 0;

            if (newData && !optionExists) {
                const newOption = new Option(
                    newData.text,
                    newData.id,
                    true,
                    true
                );
                $(selector).append(newOption);
            }

            $(selector)
                .val(newData?.id ? newData.id : null)
                .trigger('change');
        };

        // Skip confirmation if occurrence is new or has no related reports
        if (!hasRelatedReports) {
            applyChange();
            $(selector).select2('close'); // Close the dropdown
            return;
        }

        const typeCapitalized = type.charAt(0).toUpperCase() + type.slice(1);
        swal.fire({
            title: `Confirm Change of ${typeCapitalized} Name?`,
            html: `Changing this Occurrence's ${type.toLowerCase()} name will also auto-change <b>all</b> related ${constants.MODELS.OCCURRENCE_REPORT.MODEL_PREFIX} to the same name.`,
            icon: 'question',
            showCancelButton: true,
            confirmButtonText: `Confirm New ${typeCapitalized} Name`,
            reverseButtons: true,
            customClass: {
                confirmButton: 'btn btn-primary',
                cancelButton: 'btn btn-secondary',
            },
        }).then((result) => {
            if (result.isConfirmed) {
                applyChange();
            } else {
                // User cancelled the change
            }
        });
    },
};
