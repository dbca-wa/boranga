<template id="bootstrap-modal">
    <div v-show="show" :transition="transition">
        <div class="modal" data-bs-keyboard="false" data-bs-backdrop="static">
            <div
                class="modal-dialog"
                :class="[
                    modalClass,
                    {
                        'modal-dialog-scrollable': scrollable,
                        'modal-dialog-stacked': stacked,
                    },
                ]"
            >
                <div class="modal-content">
                    <!--Header-->
                    <slot name="header">
                        <div class="modal-header">
                            <h4 class="modal-title">
                                <slot name="title">
                                    {{ title }}
                                </slot>
                            </h4>
                            <button
                                type="button"
                                class="btn-close btn-close-white"
                                aria-label="Close"
                                @click="cancel"
                            ></button>
                        </div>
                    </slot>
                    <!--Container-->
                    <div class="modal-body">
                        <slot></slot>
                    </div>
                    <!--Footer-->
                    <div class="modal-footer">
                        <slot name="footer">
                            <button
                                v-if="showCancel"
                                type="button"
                                :class="cancelClass"
                                aria-label="Close"
                                @click="cancel"
                            >
                                {{ cancelText }}
                            </button>
                            <button
                                v-if="showOK"
                                id="okBtn"
                                type="button"
                                :class="okClass"
                                @click="ok"
                            >
                                {{ okText }}
                            </button>
                        </slot>
                    </div>
                </div>
            </div>
        </div>
        <div class="modal-backdrop show"></div>
    </div>
</template>

<script>
/**
 * Bootstrap Style Modal Component for Vue
 * Depend on Bootstrap.css
 */
import swal from 'sweetalert2';

export default {
    props: {
        title: {
            type: String,
            default: 'Pop-up',
        },
        small: {
            type: Boolean,
            default: false,
        },
        large: {
            type: Boolean,
            default: false,
        },
        extraLarge: {
            type: Boolean,
            default: false,
        },
        full: {
            type: Boolean,
            default: false,
        },
        force: {
            type: Boolean,
            default: false,
        },
        transition: {
            type: String,
            default: 'modal',
        },
        showOK: {
            type: Boolean,
            default: true,
        },
        showCancel: {
            type: Boolean,
            default: true,
        },
        okText: {
            type: String,
            default: 'Confirm',
        },
        cancelText: {
            type: String,
            default: 'Cancel',
        },
        okClass: {
            type: String,
            default: 'btn btn-primary',
        },
        cancelClass: {
            type: String,
            default: 'btn btn-secondary',
        },
        closeWhenOK: {
            type: Boolean,
            default: false,
        },
        dataLossWarningOnCancel: {
            type: Boolean,
            default: true,
        },
        scrollable: {
            type: Boolean,
            default: false,
        },
        stacked: {
            type: Boolean,
            default: false,
        },
    },
    data() {
        return {
            duration: null,
        };
    },
    computed: {
        modalClass() {
            return {
                'modal-xl': this.extraLarge,
                'modal-lg': this.large,
                'modal-sm': this.small,
                'modal-full': this.full,
            };
        },
        show: function () {
            return this.$parent.isModalOpen;
        },
    },
    watch: {
        show(value) {
            if (value) {
                document.body.className += ' modal-open';
            } else {
                window.setTimeout(() => {
                    document.body.className = document.body.className.replace(
                        /\s?modal-open/,
                        ''
                    );
                }, this.duration || 0);
            }
        },
    },
    created() {
        if (this.show) {
            document.body.className += ' modal-open';
        }
    },
    beforeUnmount() {
        document.body.className = document.body.className.replace(
            /\s?modal-open/,
            ''
        );
    },
    methods: {
        ok() {
            this.$emit('ok');
            if (this.closeWhenOK) {
                this.show = false;
            }
        },
        cancel() {
            if (!this.dataLossWarningOnCancel) {
                this.$parent.close();
            } else {
                swal.fire({
                    title: 'Are you sure you want to close this pop-up?',
                    text: 'You will lose any unsaved changes.',
                    icon: 'question',
                    showCancelButton: true,
                    confirmButtonText: 'Yes, close it',
                    cancelButtonText: 'Return to pop-up',
                    reverseButtons: true,
                    customClass: {
                        confirmButton: 'btn btn-primary',
                        cancelButton: 'btn btn-secondary',
                    },
                }).then((result) => {
                    if (result.isConfirmed) {
                        this.$parent.close();
                    }
                });
            }
        },
    },
};
</script>

<style scoped>
.modal {
    display: block;
    z-index: 20000 !important;
}

.modal-backdrop {
    z-index: 19999 !important;
}

.modal .btn {
    margin-bottom: 0px;
}

.modal-header {
    border-top-left-radius: 0.3rem;
    border-top-right-radius: 0.3rem;
    background-color: #226fbb;
    color: #fff;
    background: #3580ca url('/static/boranga_vue/src/parks-bg-banner.gif')
        repeat-x center bottom;
}

.btn-close {
    color: #eee;
}

.modal-footer {
    border-bottom-left-radius: 0.3rem;
    border-bottom-right-radius: 0.3rem;
}

.modal-body {
    background-color: #fff;
    color: #333333;
}

.modal-footer {
    /*background-color: #F5F5F5;
        color: #333333;*/
    background-color: #efefef;
    color: #333333;
}

.modal-transition {
    transition: all 0.6s ease;
}

.modal-leave {
    border-radius: 1px !important;
}

.modal-transition .modal-dialog,
.modal-transition .modal-backdrop {
    transition: all 0.5s ease;
}

.modal-enter .modal-dialog,
.modal-leave .modal-dialog {
    opacity: 0;
    transform: translateY(-30%);
}

.modal-enter .modal-backdrop,
.modal-leave .modal-backdrop {
    opacity: 0;
}

.close {
    font-size: 2.5rem;
    opacity: 0.3;
}

.close:hover {
    opacity: 0.7;
}

#okBtn {
    margin-bottom: 0px;
}
</style>
