<template>
    <div v-if="messages.length" class="error-renderer">
        <ul class="mb-0">
            <li v-for="(m, i) in messages" :key="i">{{ m }}</li>
        </ul>
    </div>
</template>

<script>
export default {
    name: 'ErrorRenderer',
    props: {
        errors: {
            type: [Object, String, Array],
            required: false,
            default: null,
        },
    },
    computed: {
        messages() {
            return this._flattenErrors(this.errors);
        },
    },
    methods: {
        _tryParseJson(str) {
            if (typeof str !== 'string') return str;
            const s = str.trim();
            if (!s) return s;
            // quick JSON
            try {
                return JSON.parse(s);
            } catch (e) {
                void e;
            }
            // try python-style single quotes -> double quotes
            try {
                return JSON.parse(s.replace(/'/g, '"'));
            } catch (e) {
                void e;
            }
            // unwrap surrounding quotes/brackets if present
            // use \x5b and \x5d for [ and ] to avoid unnecessary-escape lint warnings
            const unwrapped = s
                .replace(/^[\s"'\x5b\x5d]+|[\s"'\x5b\x5d]+$/g, '')
                .trim();
            return unwrapped || s;
        },

        _toString(v) {
            if (v == null) return '';
            if (typeof v === 'string') return v.trim();
            if (typeof v === 'number' || typeof v === 'boolean')
                return String(v);
            try {
                return String(v);
            } catch (e) {
                void e;
            }
            return '';
        },

        _flattenErrors(payload) {
            if (payload == null) return [];

            // If it's already an array, flatten each entry
            if (Array.isArray(payload)) {
                const out = [];
                payload.forEach((item) => {
                    this._flattenErrors(item).forEach((x) => {
                        if (x) out.push(x);
                    });
                });
                return out;
            }

            // If it's a string, try to parse JSON-like values then re-process
            if (typeof payload === 'string') {
                const parsed = this._tryParseJson(payload);
                if (parsed !== payload) return this._flattenErrors(parsed);
                // final fallback: strip stray brackets/quotes and return single message
                const cleaned = payload
                    .replace(/^[\s"'\\x5b\\x5d]+|[\s"'\\x5b\\x5d]+$/g, '')
                    .trim();
                return cleaned ? [cleaned] : [];
            }

            // If it's an object/dict, collect values (detail/non_field_errors treated same)
            if (typeof payload === 'object') {
                // special-case a single 'detail' string
                if ('detail' in payload && Object.keys(payload).length === 1) {
                    const d = this._tryParseJson(payload.detail);
                    return this._flattenErrors(d);
                }

                const out = [];
                Object.values(payload).forEach((val) => {
                    this._flattenErrors(val).forEach((x) => {
                        if (x) out.push(x);
                    });
                });
                return out;
            }

            // fallback for numbers/booleans/etc
            const s = this._toString(payload).trim();
            return s ? [s] : [];
        },
    },
};
</script>

<style scoped>
.errors-list {
    margin-bottom: 0;
}
</style>
