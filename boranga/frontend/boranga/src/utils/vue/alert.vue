<template>
    <div
        v-show="showState"
        :class="{
            alert: true,
            'alert-success': type == 'success',
            'alert-warning': type == 'warning',
            'alert-primary': type == 'primary',
            'alert-info': type == 'info',
            'alert-danger': type == 'danger',
            top: placement === 'top',
            'top-right': placement === 'top-right',
        }"
        transition="fade"
        :style="{ width: width }"
        role="alert"
    >
        <button
            v-show="dismissable"
            type="button"
            class="close"
            @click="showState = false"
        >
            <span>&times;</span>
        </button>
        <slot></slot>
    </div>
</template>

<script>
export default {
    name: 'AlertComponent',
    props: {
        type: {
            type: String,
        },
        dismissable: {
            type: Boolean,
            default: false,
        },
        show: {
            type: Boolean,
            default: true,
        },
        duration: {
            type: Number,
            default: 0,
        },
        width: {
            type: String,
        },
        placement: {
            type: String,
        },
    },
    data: function () {
        return {
            showState: true,
        };
    },
    watch: {
        show(val) {
            if (this._timeout) clearTimeout(this._timeout);
            if (val && Boolean(this.duration)) {
                this._timeout = setTimeout(() => {
                    this.showState = false;
                }, this.duration);
            }
        },
    },
    created() {
        this.showState = this.show;
    },
};
</script>

<style>
.fade-transition {
    transition: opacity 0.3s ease;
}
.fade-enter,
.fade-leave {
    height: 0;
    opacity: 0;
}
.alert.top {
    position: fixed;
    top: 30px;
    margin: 0 auto;
    left: 0;
    right: 0;
    z-index: 1050;
}
.alert.top-right {
    position: fixed;
    top: 30px;
    right: 50px;
    z-index: 1050;
}
</style>
