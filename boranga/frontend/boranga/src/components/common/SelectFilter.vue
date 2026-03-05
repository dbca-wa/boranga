<!-- A single dropdown with selectable items as a component -->
<template>
    <div class="select-filter-wrapper" :class="classes">
        <label
            v-if="showTitle"
            :for="`select-filter-${id}`"
            class="text-secondary mb-1"
            >{{ title }}</label
        >
        <Multiselect
            :id="`select-filter-${id}`"
            ref="multiselectFilter"
            v-model="selectedFilterItem"
            :mode="multiple ? 'multiple' : 'single'"
            :options="optionsFormatted"
            :label="label"
            value-prop="value"
            :placeholder="placeholder"
            :disabled="disabled"
            :searchable="true"
            :can-clear="true"
            @select="handleSelect"
            @deselect="handleDeselect"
            @change="handleChange"
            @search-change="(...args) => $emit('search', ...args)"
        />
    </div>
</template>

<script>
import Multiselect from '@vueform/multiselect';

export default {
    name: 'SelectFilter',
    components: { Multiselect },
    props: {
        id: {
            type: String,
            required: true,
        },
        title: {
            type: String,
            required: true,
        },
        options: {
            type: Array,
            required: true,
            validator: (
                /** @type {{ key: String, value: String; }[] | { value: String, text: String; }[] } */ values
            ) => {
                if (typeof values !== 'object') return false;

                return values.every((value) => {
                    const keys = Object.keys(value);
                    if (keys.length != 2) return false;
                    return (
                        (keys.includes('key') && keys.includes('value')) ||
                        (keys.includes('value') && keys.includes('text')) ||
                        (keys.includes('id') && keys.includes('name'))
                    );
                });
            },
        },
        preSelectedFilterItem: {
            type: [Number, String, Object, Array],
            required: false,
            default: () => [],
        },
        showTitle: {
            type: Boolean,
            required: false,
            default: true,
        },
        name: {
            type: String,
            required: false,
            default: 'value',
        },
        label: {
            type: String,
            required: false,
            default: 'text',
        },
        multiple: {
            type: Boolean,
            required: false,
            default: false,
        },
        placeholder: {
            type: String,
            required: false,
            default: 'Select a value',
        },
        classes: {
            type: String,
            required: false,
            default: '',
        },
        disabled: {
            type: Boolean,
            required: false,
            default: false,
        },
    },
    emits: [
        'selection-changed-select',
        'selection-changed-remove',
        'search',
        'option:selected',
        'option:deselected',
        'input',
    ],
    data: function () {
        return {
            selectedFilterItem: this.multiple ? [] : null,
        };
    },
    computed: {
        optionsFormatted: function () {
            // Allows to pass in key-value pairs or value-text pairs
            return this.mapKeyValuePairs(this.options);
        },
    },
    mounted: function () {
        // Resolve pre-selected values to the primitive value used by Multiselect (value-prop)
        const matched = this.getSelectedFilterItemByKey(
            this.preSelectedFilterItem
        );
        if (this.multiple) {
            this.selectedFilterItem = matched.map((opt) => opt.value);
        } else {
            this.selectedFilterItem =
                matched.length > 0 ? matched[0].value : null;
        }
    },
    methods: {
        /**
         * Fired by @vueform/multiselect @change — emits 'input' with the new model value.
         * @param {string|number|string[]|number[]|null} value The new model value
         */
        handleChange: function (value) {
            this.$emit('input', value);
        },
        /**
         * Fired by @vueform/multiselect @select — re-emits 'option:selected' and
         * 'selection-changed-select' to preserve backward-compatible event interface.
         * @param {string|number} _value The selected value-prop primitive (unused; option carries it)
         * @param {Object} option The full option object { value, text }
         */
        handleSelect: function (_value, option) {
            this.$emit('option:selected', option);
            this.$emit('selection-changed-select', {
                id: this.id,
                value: this.selectedFilterItem,
                multiple: this.multiple,
            });
        },
        /**
         * Fired by @vueform/multiselect @deselect — re-emits 'option:deselected' and
         * 'selection-changed-remove' to preserve backward-compatible event interface.
         * @param {string|number} _value The deselected value-prop primitive (unused; option carries it)
         * @param {Object} option The full option object { value, text }
         */
        handleDeselect: function (_value, option) {
            this.$emit('option:deselected', option);
            this.$emit('selection-changed-remove', {
                id: this.id,
                value: this.selectedFilterItem,
                multiple: this.multiple,
            });
        },
        /**
         * Maps key-value pairs to value-text pairs to be used by the Multiselect component.
         * @param {{ key: String, value: String; }[] | { value: String, text: String; }[] } options The key-value pair(s) to be mapped
         */
        mapKeyValuePairs: function (options) {
            return options.map((option) => {
                return {
                    value: Object.hasOwn(option, 'key')
                        ? option.key.toString() // Casting to string to avoid potential type mismatch
                        : Object.hasOwn(option, 'id')
                          ? option.id.toString()
                          : option.value.toString(),
                    text: Object.hasOwn(option, 'key')
                        ? option.value.toString()
                        : Object.hasOwn(option, 'name')
                          ? option.name.toString()
                          : option.text.toString(),
                };
            });
        },
        /**
         * Returns matching option objects from optionsFormatted for the given pre-selected value(s).
         * @param {Number|String|(Number|String|{value:String|Number;})[]} selected
         * @returns {{ value: string; text: string; }[]}
         */
        getSelectedFilterItemByKey: function (selected) {
            const filterOptions = this.optionsFormatted;
            if (selected === null) return [];

            return filterOptions.filter(
                (/** @type {{ value: string | number; }} */ item) => {
                    if (['number', 'string'].includes(typeof selected)) {
                        // Single value number or string
                        return item.value === selected.toString();
                    }

                    if (Array.isArray(selected)) {
                        // Array
                        if (
                            selected.length > 0 &&
                            typeof selected[0] === 'object'
                        ) {
                            // Array of objects
                            return selected.some(
                                (s) =>
                                    /** @type {{value: Number | String}} */ (
                                        s
                                    ).value.toString() === item.value
                            );
                        } else {
                            // Array of numbers or strings
                            return selected
                                .map((s) => s.toString())
                                .includes(
                                    /** @type {{value: String}} */ (item).value
                                );
                        }
                    }

                    // Object in the form of { value: string | number; text: string; }
                    if (typeof selected === 'object') {
                        return (
                            item.value ===
                            /** @type {Object} */ (selected).value?.toString()
                        );
                    }
                    return []; // Else
                }
            );
        },
    },
};
</script>

<style>
@import '@vueform/multiselect/themes/default.css';

.multiselect-dropdown {
    z-index: 9001 !important;
}

/* Fill the parent input-group-text container completely */
.select-filter-wrapper {
    width: 100%;
}

/* Override default green with project primary blue (#226fbb).
   margin:0 removes the theme's margin:0 auto which fights flex layout. */
.multiselect {
    width: 100%;
    margin: 0;
    --ms-tag-bg: #226fbb;
    --ms-tag-color: #fff;
    --ms-option-bg-selected: #226fbb;
    --ms-option-bg-selected-pointed: #5393d2;
    --ms-option-color-selected: #fff;
    --ms-option-color-selected-pointed: #fff;
    --ms-border-color-active: #226fbb;
    --ms-ring-color: rgba(34, 111, 187, 0.2);
}
</style>
