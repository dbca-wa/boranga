<template>
    <div id="app">
        <router-view></router-view>
    </div>
</template>

<script>
export default {
    name: 'App',
};
</script>

<style>
.popover {
    max-width: 100% !important;
}

textarea {
    field-sizing: content;
    resize: none;
}
.form-floating > textarea.form-control {
    height: auto;
}
.select2-container--disabled,
.select2-container--focus {
    z-index: 1;
}
.popover {
    z-index: 20001 !important;
}
</style>
