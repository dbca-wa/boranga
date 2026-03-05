<template lang="html">
    <div :id="custom_id" class="card mb-4">
        <div class="card-header h4 fw-bold p-4">
            <div
                :id="'show_hide_switch_' + section_body_id"
                class="row"
                aria-expanded="true"
                :aria-controls="section_body_id"
            >
                <div
                    class="col-11 d-flex align-items-center"
                    :style="'color:' + customColor"
                >
                    {{ label }}
                    <span
                        v-if="subtitle"
                        class="h6 m-0 p-0 ps-2"
                        :class="subtitleClass"
                        ><i
                            v-if="showSubtitleIcon"
                            :class="subtitleIconClass"
                        ></i
                        >{{ subtitle }}</span
                    >
                    <!-- to display the assessor and referral comments textboxes -->
                    <template v-if="displayCommentSection">
                        <template v-if="!isShowComment">
                            <a
                                v-if="has_comment_value"
                                href=""
                                @click.prevent="toggleComment"
                                ><i style="color: red" class="bi bi-chat"
                                    >&nbsp;</i
                                ></a
                            >
                            <a v-else href="" @click.prevent="toggleComment"
                                ><i class="bi bi-chat">&nbsp;</i></a
                            >
                        </template>
                        <a
                            v-else-if="isShowComment"
                            href=""
                            @click.prevent="toggleComment"
                            ><i class="bi bi-slash-circle">&nbsp;</i></a
                        >
                    </template>
                </div>
                <div class="col-1 text-end">
                    <i
                        :id="chevron_elem_id"
                        role="button"
                        class="bi fw-bold chevron-toggle"
                        :data-bs-target="'#' + section_body_id"
                        @click="toggle"
                    >
                    </i>
                </div>
            </div>
        </div>
        <div
            :id="section_body_id"
            :class="detailsClass"
            :style="'color:' + customColor"
        >
            <slot></slot>
        </div>
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';

export default {
    name: 'FormSection',
    props: {
        label: {},
        subtitle: {
            type: String,
            default: '',
        },
        subtitleClass: {
            type: String,
            default: 'text-muted',
        },
        showSubtitleIcon: {
            type: Boolean,
            default: false,
        },
        subtitleIconClass: {
            type: String,
            default: 'bi bi-exclamation-circle-fill text-warning me-2',
        },
        Index: {},
        hideHeader: {},
        customColor: {
            type: String,
            default: 'black',
        },
        formCollapse: {
            type: Boolean,
            default: false,
        },
        isShowComment: {
            type: Boolean,
            required: false,
        },
        has_comment_value: {
            type: Boolean,
            required: false,
        },
        displayCommentSection: {
            type: Boolean,
            default: false,
        },
    },
    emits: ['toggle', 'opened', 'collapsed', 'toggleComment'],
    data: function () {
        return {
            custom_id: uuid(),
            chevron_elem_id: 'chevron_elem_' + uuid(),
            collapsed: false,
        };
    },
    computed: {
        detailsClass: function () {
            let classText = 'card-body';
            if (this.formCollapse) {
                classText = 'card-body collapse';
            }
            return classText;
        },
        section_header_id: function () {
            return 'section_header_' + this.Index;
        },
        section_body_id: function () {
            return 'section_body_' + this.Index;
        },
    },
    mounted: function () {
        // eslint-disable-next-line no-undef
        chevron_toggle.init();
        this.collapsed = this.formCollapse;
    },
    methods: {
        toggle: function () {
            this.collapsed = !this.collapsed;
            this.$emit('toggle', this.collapsed);
            if (this.collapsed) {
                this.$emit('collapsed');
            } else {
                this.$emit('opened');
            }
        },
        toggleComment: function () {
            this.$emit('toggleComment', !this.isShowComment);
        },
    },
};
</script>
