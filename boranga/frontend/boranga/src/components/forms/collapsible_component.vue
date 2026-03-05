<template lang="html">
    <div class="toggle_filters_wrapper rounded mb-3">
        <div
            :id="button_elem_id"
            data-bs-toggle="collapse"
            :data-bs-target="'#' + target_elem_id"
            class="toggle_filters_button collapsed d-flex align-items-center"
            @click="toggle_filters_button_clicked"
        >
            <div class="me-auto ps-1 title">{{ component_title }}</div>
            <div v-if="showWarningIcon" class="me-2">
                <i
                    :id="warning_icon_id"
                    :title="warning_icon_title"
                    class="bi bi-exclamation-circle fs-2 filter_warning_icon"
                ></i>
            </div>
            <div class="me-2">
                <i
                    :id="chevron_elem_id"
                    class="rotate_icon bi bi-chevron-down"
                ></i>
            </div>
        </div>

        <div
            :id="target_elem_id"
            class="collapse border-top mt-1"
            :class="collapsed ? 'collapse' : 'collapse show'"
            :aria-expanded="collapsed ? 'false' : 'true'"
        >
            <slot></slot>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
export default {
    name: 'CollapsibleComponent',
    props: {
        component_title: {
            type: String,
            required: false,
            default: '',
        },
        collapsed: {
            type: Boolean,
            default: true,
        },
        showWarningIcon: {
            type: Boolean,
            default: false,
        },
    },
    data: function () {
        return {
            target_elem_id: 'target_elem_' + uuid(),
            button_elem_id: 'button_elem_' + uuid(),
            chevron_elem_id: 'chevron_elem_' + uuid(),
            warning_icon_id: 'warning_elem_' + uuid(),
            warning_icon_title: '',
            display_icon: false,
            filters_expanded: null,
        };
    },
    watch: {
        filters_expanded: function () {
            let chevron_icon = $('#' + this.chevron_elem_id);
            if (this.filters_expanded) {
                chevron_icon.addClass('chev_rotated');
            } else {
                chevron_icon.removeClass('chev_rotated');
            }
        },
    },
    mounted: function () {
        this.$nextTick(function () {
            this.$emit('created');
        });
    },
    methods: {
        toggle_filters_button_clicked: function () {
            // Bootstrap add a 'collapsed' class name to the element
            let filters_expanded_when_clicked = $(
                '#' + this.button_elem_id
            ).hasClass('collapsed');
            this.filters_expanded = !filters_expanded_when_clicked;
        },
    },
};
</script>

<style lang="css">
.toggle_filters_wrapper {
    background: #efefee;
    padding: 0.5em;
    display: grid;
    color: #505050;
}

.toggle_filters_wrapper .form-group {
    margin-top: 0.6em;
    margin-bottom: 0.6em;
    padding-left: 0.4em;
    padding-right: 0.4em;
}

.toggle_filters_wrapper .form-group .form-select,
.toggle_filters_wrapper .form-group .form-control {
    color: #6c757d;
}

.toggle_filters_button {
    cursor: pointer;
}

.filter_warning_icon {
    color: #ffc107;
    transition: 0.5s;
}

.rotate_icon {
    transition: 0.5s;
}

.chev_rotated {
    transform: rotate(180deg);
}

.title,
.rotate_icon {
    color: #505050;
}
</style>
